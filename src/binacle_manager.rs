extern crate rustc_serialize;
extern crate fs2;
extern crate walkdir;

use std::fs::{File, read_dir, metadata};
use std::path::PathBuf;
use std::fs::OpenOptions;
use self::fs2::FileExt;
use std::io::*;
use std::ptr;
use std::collections::{HashSet, HashMap};
use rustc_serialize::json;
use walkdir::WalkDir;

use binacle::BinacleFile;


// Used to maintain the Binacle Files
pub struct BinacleManager {
	pub db_path: String,
    cur_index: Option<(usize, BinacleFile)>,
	meta: BinacleMeta,
    map: Option<HashMap<u32, String>>,
}

#[derive(RustcDecodable, RustcEncodable)]
struct BinacleMeta {
    is_map: bool, 
	nb_file: u32,
	last_id: u32,
    max_index_size: u64,
    // size of a list offset in the header
    offset_size: u8, // in bytes, in [4 .. 6], 5 is OK.
    // OPTIM: list are aligned on 2**alignment
    alignment: u8, // in bits, pow of 2, in [4 .. 12], 6 is OK.
    // size of a ngram
    ngram_size: u8, // in bits, in [12..32], 28 is OK.
	index: Vec<BinacleIndex>,
}

#[derive(RustcDecodable, RustcEncodable, Clone)]
struct BinacleIndex {
	path: String,
	is_full: bool,
}


#[allow(dead_code)]
impl BinacleManager {

    // create a new manager, max_index_size should be 80% of the available RAM
	pub fn create(path: &str, use_map: bool, max_index_size: u64, alignment: u8, ngram_size: u8) -> Result<BinacleManager> {

        let mut file = try!(OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(path));

        try!(file.lock_exclusive());

		let meta = BinacleMeta {
            is_map: use_map,
			nb_file: 0,
            last_id: 0,
            max_index_size: max_index_size,
            offset_size: 5,
            alignment: alignment,
            ngram_size: ngram_size,
		    index: Vec::new(),
		};

        let map = match use_map {
            true => Some(HashMap::new()),
            false => None,
        };

        BinacleManager::write_meta(&mut file, &meta);

		Ok(BinacleManager { 
            db_path: String::from(path),
            cur_index: None,
            meta: meta,
            map: map,
        })
	}

    // open a BinacleManager File
	pub fn open(path: &str) -> Result<BinacleManager> {

        let mut file = try!(OpenOptions::new()
                    .read(true)
                    .open(path));

        try!(file.lock_exclusive());

        let meta = try!(BinacleManager::read_meta(&mut file));
        let map = match meta.is_map {
            false => None,
            true => Some(try!(BinacleManager::read_map(path))),
        }; 

        Ok(BinacleManager {
            db_path: String::from(path),
            cur_index: None,
            meta: meta,
            map: map,
        })
    }

    // insert only one file in the database
    // do NOT use this when you want to insert several files
    pub fn insert_file(&mut self, filepath: &str, id: u32, update_map: bool) -> Result<u32> {

        match self.cur_index {
            Some((i, ref mut db)) if !self.meta.index[i].is_full => {
                try!(db.insert_file(filepath, id));
                
                self.meta.nb_file += 1;
                self.meta.last_id = id;

                if db.get_size() > self.meta.max_index_size {
                    self.meta.index[i].is_full = true;
                    db.fix_size();
                }

                if self.meta.is_map {
                    if let Some(ref mut h) = self.map {
                        h.insert(id, String::from(filepath));
                    }
                };
            },

            Some(_) | None => {
                try!(self.set_cur_index());
                try!(self.insert_file(filepath, id, update_map));
            },
        };

        if self.meta.is_map && update_map {
            self.write_map();
        }
        
        Ok(0)
    }

    // insert several files in the database
    pub fn insert_files(&mut self, files: Vec<(u32, &str)>) -> Result<()> {
    
        for file in &files {
            let _ = try!(self.insert_file(file.1, file.0, false));
        }

        if self.meta.is_map {
            self.write_map();
        }
        
        Ok(())
    }

    // insert all files in a directory, recursively
    pub fn insert_dir_recursive(&mut self, dir: &str) -> Result<()> {

        let _ = try!(read_dir(dir));
        let mut id = self.meta.last_id + 1;

        for entry in WalkDir::new(dir) {
            
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };

            let p = entry.path();
            let meta = match metadata(&p) {
                Ok(s) => s,
                Err(_) => continue,
            };

            if !meta.is_file() {
                continue;
            }

            let size = meta.len();

            match p.to_str() {
                Some(file) => {
                    let _ = self.insert_file(file, id, false);
                    id += 1;                
                },
                None => continue,
            }

            if id % 100 == 0 {
                println!("Inserting file {} (size {}) {:?}", id, size, p);
            }
        }

        if self.meta.is_map {
            self.write_map();
        }

        Ok(())
    }

    // search all files that match the pattern
    pub fn search(&mut self, pattern: &[u8]) -> Result<HashSet<u32>> {

        // close the cur_index in order to open all index in read only
        self.cur_index = None;

        // search on all index and make the union
        let mut set_ids = HashSet::new();

        for index in &self.meta.index {

            let db = try!(BinacleFile::open_read(&index.path));
            let ids = try!(db.search(pattern));
            set_ids.extend(ids);
        }

        Ok(set_ids)
    }

    pub fn search_multi(&mut self, patterns: & [Vec<u8>]) -> Result<HashSet<u32>> {

        // transform all patterns in a set of ngrams
        let mut ngram_set = HashSet::new();
        for p in patterns {

            if p.len() < 4 {
                return Err(Error::new(ErrorKind::Other, "pattern size is < 4"));
            }

            for i in 0 .. p.len()-3 {
                let ptr_read = (&p).as_ptr() as u64 + i as u64;
                let ngram: u32 = unsafe { ptr::read(ptr_read as *const u32)};
                ngram_set.insert(ngram);
            }
        }

        // close the cur_index to open all index in read only
        self.cur_index = None;

        // search on all indexes and do the union
        let mut set_ids = HashSet::new();

        for index in &self.meta.index {

            let db = try!(BinacleFile::open_read(&index.path));
            let ids = try!(db.search_ngrams(&ngram_set));
            set_ids.extend(ids);
        }
        Ok(set_ids)
    }

    pub fn to_map(&self, ids: &HashSet<u32>) -> Result<Vec<String>> {

        assert!(self.meta.is_map);
        let mut res = Vec::with_capacity(ids.len());

        if let Some(ref map) = self.map {
            for id in ids {
                match map.get(id) {
                    None => panic!("Not found in map"),
                    Some(file) => res.push(file.to_owned()),
                }
            }
        }
        Ok(res)
    }

    pub fn is_map(&self) -> bool {
        return self.meta.is_map;
    }

    /*********************/
    /*  Private methods  */
    /*********************/

    fn set_cur_index(&mut self) -> Result<()> {

        let free_index = self.meta.index.iter().cloned().enumerate().find(|x| !x.1.is_full);

        match free_index {
            Some((i, index)) => {
                self.cur_index = Some((i, try!(BinacleFile::open_write(&index.path))));
            },
            None => {
                try!(self.add_index());
            },
        };
        Ok(())
    }

    fn add_index(&mut self) -> Result<()> {

        let offset_size = self.meta.offset_size;
        let alignment = self.meta.alignment;
        let ngram_size = self.meta.ngram_size;

    	if offset_size < 4 || offset_size > 8 {
    		return Err(Error::new(ErrorKind::Other, "violation: 4 <= offset_size <= 8"));
    	} else if alignment < 4 || alignment > 12 {
    		return Err(Error::new(ErrorKind::Other, "violation: 4 <= alignement <= 12"));
    	} else if ngram_size < 14 || ngram_size > 32 {
    		return Err(Error::new(ErrorKind::Other, "violation: 14 <= ngram_size <= 32"));
    	}

    	let index_nb = self.meta.index.len();
        let index_name = format!("{}_index{}.db", self.db_path, index_nb);
        let binacle = try!(BinacleFile::create(&index_name, offset_size, alignment, ngram_size));
    	let index = BinacleIndex {
			path: index_name,
			is_full: false,
    	};

    	self.meta.index.push(index);
        self.cur_index = Some((self.meta.index.len()-1, binacle));
    	Ok(())
    }

    fn read_meta(file: &mut File) -> Result<BinacleMeta> {

	    let mut encoded = String::new();

	    try!(file.seek(SeekFrom::Start(0)));
	    file.read_to_string(&mut encoded).unwrap();

	    let meta = json::decode(&encoded).unwrap();

	    Ok(meta)
    }

    fn write_meta(file: &mut File, meta: &BinacleMeta) {
        let encoded = json::encode(meta).unwrap();

        file.seek(SeekFrom::Start(0)).unwrap();
        let _ = file.write_all(&encoded.into_bytes());    
    }

    fn read_map(path: &str) -> Result<HashMap<u32, String>> {

        let mut meta_path = PathBuf::from(&path);
        meta_path.set_extension("map");

        let file = OpenOptions::new()
                    .read(true)
                    .open(meta_path.to_str().unwrap());

        match file {
            Err(e) => {
                return Err(e);
            },
            Ok(mut file) => {
                let mut encoded = String::new();
                try!(file.read_to_string(&mut encoded));
                return Ok(json::decode(&encoded).unwrap());
            }
        }
    }

    fn write_map(&self) {

        assert!(self.meta.is_map);

        let encoded = json::encode(&self.map).unwrap();

        let mut path = PathBuf::from(&self.db_path);
        path.set_extension("map");

        let mut file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(path.to_str().unwrap())
                    .unwrap();

        let _ = file.write_all(&encoded.into_bytes());    
    }

}

impl Drop for BinacleManager {

    fn drop(&mut self) {

        let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(false)
                    .open(&self.db_path)
                    .unwrap();

        // lock the file to prevent parallel use
        file.lock_exclusive().unwrap();
        
        BinacleManager::write_meta(&mut file, &self.meta);
        if self.meta.is_map {
            self.write_map();
        }
    }
}