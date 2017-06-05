extern crate memmap;
extern crate rustc_serialize;
extern crate fs2;

use std::fs::{self, File};
use std::io::*;
use std::ptr;
use std::fs::OpenOptions;
use std::cmp::{min, max};
use std::collections::{HashSet};
use std::path::PathBuf;
use rustc_serialize::json;

use self::memmap::{Mmap, Protection};
use self::fs2::FileExt;

pub struct BinacleFile {
    pub path: String,
    filesize: u64,
    file: File,
    map: Mmap,
    raw: BinacleStruct,
}

#[derive(Clone, RustcDecodable, RustcEncodable)]
pub struct BinacleStruct {
    size: u64, // do not specify
    offset_size: u8, // in bytes, in [4 .. 6]
    alignment: u8, // in bits, take the pow of 2, in [4 .. 12]
    ngram_size: u8, // in bits, in [24 .. 32]
    nb_file: u32,
    last_id: u32,
    average_size: f64,
}

#[allow(dead_code)]
impl BinacleFile {

    pub fn create(path: &str, offset_size: u8, alignment: u8, ngram_size: u8) -> Result<BinacleFile> {
        
        let file = try!(OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(path));

        // lock the file to prevent parallel use
        try!(file.lock_exclusive());

        let mut header = BinacleStruct {
            size: 0,
            offset_size: offset_size,
            alignment: alignment,
            ngram_size: ngram_size,
            nb_file: 0,
            last_id: 0,
            average_size: 0.0,
        };

        let mut size = header.offset_size as u64 * (1u64 << header.ngram_size);
        size += 2u64.pow(header.alignment as u32) - (size % 2u64.pow(header.alignment as u32));
        header.size = size;
        
        let _ = file.set_len(size);

        // create a meta file
        BinacleFile::write_meta(&path, &header);

        let mmap = Mmap::open(&file, Protection::ReadWrite).unwrap();

        let meta = BinacleFile::read_meta(&String::from(path));

        Ok(BinacleFile { 
            path: String::from(path),
            filesize: size,
            file: file,
            map: mmap,
            raw: meta,
        })
    }

    // constructor
    // open a database file, read only
    pub fn open_read(path: &str) -> Result<BinacleFile> {

        let file = try!(OpenOptions::new()
                    .read(true)
                    .open(path));

        // allow parallel reads but no write
        try!(file.lock_shared());      

        let size = file.metadata().unwrap().len() as u64; 

        let mmap = Mmap::open(&file, Protection::Read).unwrap();

        let meta = BinacleFile::read_meta(&String::from(path));

        Ok(BinacleFile { 
            path: String::from(path),
            filesize: size,
            file: file,
            map: mmap,
            raw: meta,
        })
    }

    pub fn open_write(path: &str) -> Result<BinacleFile> {
        
        let file = try!(OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(false)
                    .open(path));

        // lock the file to prevent parallel use
        try!(file.lock_exclusive());

        let size = file.metadata().unwrap().len() as u64;

        let mmap = Mmap::open(&file, Protection::ReadWrite).unwrap();

        let meta = BinacleFile::read_meta(&String::from(path));

        Ok(BinacleFile { 
            path: String::from(path),
            filesize: size,
            file: file,
            map: mmap,
            raw: meta,
        })
    }


    // insert a file from its filepath
    pub fn insert_file(&mut self, filepath: &str, id: u32) -> Result<u32> {

        let mut file = try!(OpenOptions::new().read(true).open(filepath));
        let size = try!(fs::metadata(&filepath)).len() as u32;

        let mut buf = vec![0u8; 4096*256];
        loop {
            match file.read(&mut buf).unwrap() {
                0 => break,
                1 | 2 | 3 => break,
                len => {
                    for i in 0 .. len-3 {
                        let ptr_read = buf.as_ptr() as u64 + i as u64;
                        let ngram = unsafe { ptr::read(ptr_read as *const u32)};
                        let _ = self.insert_ngram(id, ngram);
                    }
                }
            }
        }

        // update the meta file
        self.raw.average_size = (self.raw.average_size * self.raw.nb_file as f64 + size as f64) / (self.raw.nb_file + 1) as f64;
        self.raw.nb_file += 1;
        self.raw.last_id = id;
        
        Ok(id)
    }

    // find all the files that contain all the ngrams
    pub fn search_ngrams(&self, ngrams: &HashSet<u32>) -> Result<HashSet<u32>> {

        // for each ngram, get the #id
        let mut ngram_to_nb = Vec::with_capacity(ngrams.len());
        for ngram in ngrams {
            let nb = self.get_ids_size_by_ngram(*ngram);
            ngram_to_nb.push((ngram, nb));
        }

        // sort to optimize index HashSet size
        // take the smallest list first, intersection will be faster
        ngram_to_nb.sort_by(|a, b| a.1.cmp(&b.1));

        let mut set = self.get_ids_by_ngram(*ngram_to_nb[0].0);

        for i in 1 .. ngram_to_nb.len() {

            if set.is_empty() {
                break;
            }
            set = self.intersect_ids_by_ngram(set, *ngram_to_nb[i].0);
        }  
        Ok(set)  

    }

    // find all file ids containing the pattern
    // split the pattern into a set of ngrams
    pub fn search(&self, pattern: &[u8]) -> Result<HashSet<u32>> {

        // split the search pattern in 4-grams
        if pattern.len() < 4 {
            return Err(Error::new(ErrorKind::Other, "pattern size is < 4"));
        }

        let mut ngram_set = HashSet::with_capacity(pattern.len()-3);

        for i in 0 .. pattern.len()-3 {
            let ptr_read = pattern.as_ptr() as u64 + i as u64;
            let ngram: u32 = unsafe { ptr::read(ptr_read as *const u32)};
            ngram_set.insert(ngram);
        }

        self.search_ngrams(&ngram_set)
    }


    pub fn get_ids_by_ngram(&self, ngram: u32) -> HashSet<u32> {

        let mut list_off = self.ngram_list_ptr(ngram);
        let mut set: HashSet<u32> = HashSet::new();
        
        while list_off != 0 {

            let set_from_list = self.unpack_list(list_off);
            let prev_off = self.get_list_meta(list_off).3;
            list_off = prev_off;

            set.extend(&set_from_list);
        }
        set
    }

    pub fn intersect_ids_by_ngram(&self, set: HashSet<u32>, ngram: u32) -> HashSet<u32> {

        let mut list_off = self.ngram_list_ptr(ngram);
        let mut new_set: HashSet<u32> = HashSet::with_capacity(set.len());
        
        while list_off != 0 {

            let set_from_list = self.unpack_list(list_off);
            let prev_off = self.get_list_meta(list_off).3;
            list_off = prev_off;

            new_set.extend(set.intersection(&set_from_list));

            if new_set.len() == set.len() {
                break;
            }
        }
        new_set
    }

    pub fn get_ids_size_by_ngram(&self, ngram: u32) -> u32 {

        let mut list_off = self.ngram_list_ptr(ngram);
        let mut nb_total = 0;

        while list_off != 0 {
            let (_, nb_elem, _, prev_off) = self.get_list_meta(list_off);
            nb_total += nb_elem as u32;
            list_off = prev_off;
        }
        nb_total
    }

    pub fn get_size(&self) -> u64 {
        return self.raw.size;
    }

    pub fn fix_size(&mut self) {
        let _ = self.file.set_len(self.raw.size);
        self.filesize = self.raw.size;
        self.map = Mmap::open(&self.file, Protection::ReadWrite).unwrap();
    }


    /*********************/
    /*  Private methods  */
    /*********************/

    fn insert_ngram(&mut self, id: u32, ngram: u32) -> Result<u16> {

        // check if the list is already allocated
        let mut list_off = self.ngram_list_ptr(ngram);

        if list_off == 0 {
            list_off = self.alloc_list(ngram);
        }

        // check if the list is large enough to store one more element
        let (mut size_log, mut nb_elem, mut nb_bytes, mut prev_off) = self.get_list_meta(list_off);

        if 2u64.pow(size_log as u32) < nb_bytes as u64 + 4 /* one more elem */ + 5 + self.raw.offset_size as u64 {
            let relist = self.realloc_list(list_off, ngram);
            prev_off = list_off;
            list_off = relist.0;
            size_log = relist.1;
            nb_elem = 0;
            nb_bytes = 0;
        }

        //update_list
        let mut list_ptr = self.map.ptr() as u64;
        list_ptr += list_off;
        list_ptr += 5u64 + self.raw.offset_size as u64 + nb_bytes as u64; 

        // do not insert if last id is the same
        if nb_elem != 0 {
            let mut last_id_ptr = list_ptr - 4;
            let last_id = unsafe { ptr::read(last_id_ptr as *const u32)};
            
            // id == last_id => id already in, so we return
            if last_id == id {
                return Ok(nb_elem);

            // we need to insert a new id
            } else {

                let (packed, pack_size) = BinacleFile::pack_integer(id - last_id);
  
                // we keep the first elem intact
                if nb_elem == 1 {
                    last_id_ptr += 4;
                    nb_bytes += 4;
                } 

                unsafe { 

                    // copy the packed int
                    // replace the last id (except if its the first element of the elem, see behind)
                    ptr::copy_nonoverlapping(
                        &packed as *const u32 as *const u8, 
                        last_id_ptr as *const u64 as *mut u8, 
                        pack_size as usize);

                    // then copy the elem itself (to allow hot insert)
                    last_id_ptr += pack_size as u64;
                    ptr::copy_nonoverlapping(
                        &id as *const u32, 
                        last_id_ptr as *const u64 as *mut u32, 
                        1);
                };
                nb_bytes += pack_size as u16;
            }

        } else {

            unsafe { ptr::copy_nonoverlapping(
                        &id as *const u32, 
                        list_ptr as *const u64 as *mut u32, 
                        1);
            };
            nb_bytes += 4;
        }

        self.update_list_meta(list_off, size_log, nb_elem+1, nb_bytes, prev_off);
        Ok(nb_elem+1)
    }

    // take an u32, compute the representation using var encoding
    // if the msb is 1, need one more byte
    // max number is 268435455 (takes 4 bytes)
    fn pack_integer(int: u32) -> (u32, u8) {

        let b1 = (int & 0x7F) << 0;
        let b2 = (int & 0x3F80) << 1;
        let b3 = (int & 0x1FC000) << 2;
        let b4 = (int & 0x0FE00000) << 3;

        if int < 128 {
            return (b1, 1);

        } else if int < 16384 {
            return (b1 | 0x80 | b2, 2);

        } else if int <= 2097152 {
            return (b1 | 0x80 | b2 | 0x8000 | b3, 3);

        } else if int <= 268435456 {
            return (b1 | 0x80 | b2 | 0x8000 | b3 | 0x800000 | b4, 4);
            
        } else {
            panic!("pack integer: more than 2**28");
        }
    }

    fn unpack_integer(int: u32) -> (u32, u8) { 
        let b1 = (int & (0x7F << 0)) >> 0;
        let b2 = (int & (0x7F << 8)) >> 1;
        let b3 = (int & (0x7F << 16)) >> 2;
        let b4 = (int & (0x7F << 24)) >> 3;

        if int & 0x80 == 0 {
            return (b1 as u32, 1);

        } else if int & 0x8000 == 0 {
            return ((b1 | b2) as u32, 2);

        } else if int & 0x800000 == 0 {
            return ((b1 | b2 | b3) as u32, 3);

        } else {
            return ((b1 | b2 | b3 | b4) as u32, 4);
        }
    }

    fn unpack_list(&self, list_off: u64) -> HashSet<u32> {

        let list_ptr = self.map.ptr() as u64;
        let mut nb_elem = self.get_list_meta(list_off).1;
        
        if nb_elem == 0 {
            return HashSet::new();
        }
        
        let mut set = HashSet::with_capacity(nb_elem as usize);
        let mut cur_ptr_list = list_ptr + list_off + 5 + self.raw.offset_size as u64;
        let mut cur_elem = unsafe { ptr::read(cur_ptr_list as *const u32)};
        cur_ptr_list += 4;
        nb_elem -= 1;
        set.insert(cur_elem);

        while nb_elem > 0 {
            let next = unsafe { ptr::read(cur_ptr_list as *const u32)};
            let (diff, nb_bytes) = BinacleFile::unpack_integer(next);
            cur_ptr_list += nb_bytes as u64;
            cur_elem += diff;
            set.insert(cur_elem);
            nb_elem -= 1;
        }

        return set;
    }

    fn read_meta(path: &str) -> BinacleStruct {

        let mut meta_path = PathBuf::from(&path);
        meta_path.set_extension("meta");

        let mut file = OpenOptions::new()
                    .read(true)
                    .open(meta_path.to_str().unwrap())
                    .unwrap();

        let mut encoded = String::new();
        file.read_to_string(&mut encoded).unwrap();

        let meta = json::decode(&encoded).unwrap();

        meta
    }

    fn write_meta(path: &str, meta: &BinacleStruct) {
        let encoded = json::encode(meta).unwrap();

        let mut path = PathBuf::from(&path);
        path.set_extension("meta");

        let mut file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(path.to_str().unwrap())
                    .unwrap();

        let _ = file.write_all(&encoded.into_bytes());    
    }

    
    fn incr_size(&mut self, incr_size: u64) {
        let _ = self.file.set_len(self.filesize + incr_size);
        self.filesize += incr_size;
        self.map = Mmap::open(&self.file, Protection::ReadWrite).unwrap();
    }



    fn reduce_ngram(&self, ngram: u32) -> u32 {
        ngram & ((1u64 << self.raw.ngram_size) - 1) as u32
    }

    #[inline(always)]
    fn ngram_list_ptr(&self, ngram_f: u32) -> u64 {

        let ngram = self.reduce_ngram(ngram_f);

        // rcompute offset of ngram list in header
        let mut offset = self.map.ptr() as u64; 
        offset += self.raw.offset_size as u64 * ngram as u64;

        // read the offset in the header
        let mut list_off = unsafe { ptr::read(offset as *const u64)};

        // reduce list_ptr according to offset_size
        list_off &= (1u64 << (self.raw.offset_size*8)) - 1;

        // align the list_ptr on alignement
        list_off <<= self.raw.alignment;

        list_off
    }

    fn get_new_free_list(&mut self, size_log: u8) -> u64 {

        let list_size = 2u64.pow(size_log as u32);

        if (self.raw.size + list_size) >= self.filesize {
            self.incr_size(max(512*1024*1024, list_size));
        };

        // we alloc the new list at the end of the list
        let list_off = self.raw.size;

        // update the size of the DB to handle the new list size
        self.raw.size += list_size;

        list_off
    }

    fn realloc_list(&mut self, list_off: u64, ngram: u32) -> (u64, u8) {

        let (size_log, nb, _, _) = self.get_list_meta(list_off);

        let new_size_log = min(size_log + 1, 12);

        // get a new free bloc a requested size
        let new_list_off = self.get_new_free_list(new_size_log);

        // write the new list_ptr into the header
        self.update_header(ngram, new_list_off);

        // update the size of the new list
        self.update_list_meta(new_list_off, new_size_log as u8, nb, 0, list_off);

        (new_list_off, new_size_log)
    }

    fn alloc_list(&mut self, ngram_f: u32) -> u64 {
        
        let ngram = self.reduce_ngram(ngram_f);
        let list_size_log = self.raw.alignment;

        // we look throuh the map to see if a free list is available
        let list_off = self.get_new_free_list(list_size_log);
        
        // write the new list_ptr into the header
        self.update_header(ngram, list_off);

        // init the new list with size and nb_elem
        self.update_list_meta(list_off, list_size_log as u8, 0, 0, 0);

        list_off 
    }

    fn update_header(&mut self, ngram_f: u32, mut list_off: u64) {
        let ngram = self.reduce_ngram(ngram_f);

        // compute the offset in the header
        let mut offset = self.map.ptr() as u64; 
        offset += self.raw.offset_size as u64 * ngram as u64;

        list_off >>= self.raw.alignment;

        // copy offset_size byte of list_off
        unsafe { ptr::copy_nonoverlapping(
                    &list_off as *const u64 as *const u8, 
                    offset as *const u64 as *mut u8, 
                    self.raw.offset_size as usize);
        };
    }

    #[inline(always)]
    fn update_list_meta(&mut self, list_off: u64, size: u8, nb: u16, nb_bytes: u16, mut prev_off: u64) {

        let mut list_ptr = self.map.ptr() as u64;
        list_ptr += list_off; 

        unsafe { 
            ptr::copy_nonoverlapping(
                &size as *const u8, 
                list_ptr as *const u64 as *mut u8,
                1);

            list_ptr += 1;
            ptr::copy_nonoverlapping(
                &nb as *const u16, 
                list_ptr as *const u64 as *mut u16,
                1); 

            list_ptr += 2;
            ptr::copy_nonoverlapping(
                &nb_bytes as *const u16, 
                list_ptr as *const u64 as *mut u16,
                1); 

            list_ptr += 2;

            prev_off >>= self.raw.alignment;

            ptr::copy_nonoverlapping(
                &prev_off as *const u64 as *const u8, 
                list_ptr as *const u64 as *mut u8,
                self.raw.offset_size as usize); 

        };
    }

    #[inline(always)]
    fn get_list_meta(&self, list_off: u64) -> (u8, u16, u16, u64) {

        let mut list_ptr = self.map.ptr() as u64;
        list_ptr += list_off;

        let size = unsafe { ptr::read(list_ptr as *const u8)};
        list_ptr += 1;

        let nb_id = unsafe { ptr::read(list_ptr as *const u16)};
        list_ptr += 2;

        let nb_bytes = unsafe { ptr::read(list_ptr as *const u16)};
        list_ptr += 2;

        let mut prev_off = unsafe { ptr::read(list_ptr as *const u64)};
        prev_off &= (1u64 << (self.raw.offset_size * 8)) - 1;
        prev_off <<= self.raw.alignment;

        (size, nb_id, nb_bytes, prev_off)
        
    }
}


impl Drop for BinacleFile {

    fn drop(&mut self) {
        let meta = self.raw.clone();
        BinacleFile::write_meta(&self.path, &meta);
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use std::fs::{remove_file};
    use std::fs::OpenOptions;
    use std::ptr;
    use std::io::*;
    use std::panic::{self, AssertUnwindSafe};

    fn verify_file(database: &BinacleFile, filepath: &str, id: u32) -> Result<u32> {

        let mut file = try!(OpenOptions::new().read(true).open(filepath));

        let mut buf = vec![0u8; 4096*64];
        loop {
            match file.read(&mut buf).unwrap() {
                0 => break,
                1 | 2 | 3 => break,
                len => {
                    for i in 0 .. len-3 {
                        let ptr_read = buf.as_ptr() as u64 + i as u64;
                        let ngram: u32 = unsafe { ptr::read(ptr_read as *const u32)};

                        assert_eq!(1, database.get_ids_size_by_ngram(ngram));

                        let m = database.get_ids_by_ngram(ngram);
                        assert!(m.contains(&id)); 
                    }
                }
            }
        }
        Ok(id)
    }

    #[test]
    fn insert_file_1() {
        {
            let mut db = BinacleFile::create("test_file1.db", 5, 6, 28).unwrap();
            let _ = db.insert_file("Cargo.lock", 0x12345678).unwrap();
            let _ = verify_file(&db, "Cargo.lock", 0x12345678).unwrap();

        }
        let _ = remove_file("test_file1.db");
        let _ = remove_file("test_file1.meta");
    }

    #[test]
    fn init_size() {
        {
            let db = BinacleFile::create("test1.db", 5, 6, 28).unwrap();
            let mut expected_size = db.raw.offset_size as u64 * (1u64 << db.raw.ngram_size);
            expected_size += 2u64.pow(db.raw.alignment as u32) - (expected_size % 2u64.pow(db.raw.alignment as u32));

            let size = db.file.metadata().unwrap().len();
            
            assert_eq!(0, size % 2u64.pow(db.raw.alignment as u32));
            assert_eq!(expected_size, size);            
        }
        let _ = remove_file("test1.db");
        let _ = remove_file("test1.meta");
    }

    #[test]
    fn read_header() {
        {
            let db = BinacleFile::create("test2.db", 5, 6, 28).unwrap();
            let hd = BinacleStruct {
                size: 0,
                offset_size: 5,
                alignment: 6,
                ngram_size: 28,
                last_id: 0,
                nb_file: 0,
                average_size: 0.0,
            };
            assert_eq!(db.raw.size, db.file.metadata().unwrap().len());
            assert_eq!(hd.offset_size, db.raw.offset_size);
            assert_eq!(hd.alignment, db.raw.alignment);
            assert_eq!(hd.ngram_size, db.raw.ngram_size);            
        }
        let _ = remove_file("test2.db");
        let _ = remove_file("test2.meta");
    }

    fn helper_insert(mut db: &mut BinacleFile, id: u32, ngram: u32, size: u32) {

        for i in 0 .. size {
            let _ = db.insert_ngram(id+i, ngram);    
        }
        
        let m = db.get_ids_by_ngram(ngram);

        assert_eq!(m.len(), size as usize);

        for i in 0 .. size {
            let mid = id+i;
            assert!(m.contains(&mid));    
        }
    }

    fn helper_insert2(mut db: &mut BinacleFile, id: u32, ngram: u32, size: u32) {

        for i in 0 .. size {
            let _ = db.insert_ngram(id, ngram+i);    
        }

        for i in 1 .. size {
            let m = db.get_ids_by_ngram(ngram+i);
            assert_eq!(m.len(), 1);
        
            assert!(m.contains(&id));    
        }
    }

    #[test]
    fn insert_ngram_1() {
        {
            let mut db = BinacleFile::create("test3.db", 5, 6, 28).unwrap();
            helper_insert(&mut db, 0x12345678, 0xABCDEF12, 1);            
        }
        let _ = remove_file("test3.db");
        let _ = remove_file("test3.meta");
    }

    #[test]
    fn insert_mult() { 
        {
            let mut db = BinacleFile::create("test4.db", 5, 6, 28).unwrap();
            helper_insert(&mut db, 0x1337, 0x13874763, 256);            
        }

        let _ = remove_file("test4.db");
        let _ = remove_file("test4.meta");
    }

    #[test]
    fn insert_id_big() {
        {
            let mut db = BinacleFile::create("test5.db", 5, 6, 28).unwrap();
            helper_insert(&mut db, 0x1337, 0x78747634, 255000);
        }
        let _ = remove_file("test5.db");
        let _ = remove_file("test5.meta");
    }

    #[test]
    fn insert_id_medium() {
        {
            let mut db = BinacleFile::create("test0.db", 5, 6, 28).unwrap();
            helper_insert(&mut db, 0x1337, 0x78747634, 4096);           
        }

        let _ = remove_file("test0.db");
        let _ = remove_file("test0.meta");
    }

    #[test]
    fn insert_corner_case_ngram1() { 
        {
            let mut db = BinacleFile::create("test6.db", 5, 6, 28).unwrap();
            helper_insert(&mut db, 0x66778899, 0x0, 256);            
        }

        let _ = remove_file("test6.db");
        let _ = remove_file("test6.meta");
    }

    #[test]
    fn insert_corner_case_ngram2() {
        {
            let mut db = BinacleFile::create("test7.db", 5, 6, 28).unwrap();
            helper_insert(&mut db, 0x12345678, 0xFFFFFFFF, 256);            
        }

        let _ = remove_file("test7.db");
        let _ = remove_file("test7.meta");
    }

    #[test]
    fn insert_corner_case_id1() {
        {
            let mut db = BinacleFile::create("test8.db", 5, 6, 28).unwrap();
            helper_insert(&mut db, 0x0, 0x66778899, 256);            
        }

        let _ = remove_file("test8.db");
        let _ = remove_file("test8.meta");
    }

    #[test]
    fn insert_corner_case_id2() {
        {
            let mut db = BinacleFile::create("test9.db", 5, 6, 28).unwrap();
            helper_insert(&mut db, 0xFFFFFF00, 0xAA778899, 256);            
        }

        let _ = remove_file("test9.db");
        let _ = remove_file("test9.meta");
    }

    #[test]
    fn insert_ngram_big() {
        {
            let mut db = BinacleFile::create("test10.db", 5, 6, 28).unwrap();
            helper_insert2(&mut db, 0x12345678, 0x0, 1024*1024);            
        }

        let _ = remove_file("test10.db");
        let _ = remove_file("test10.meta");
    }

    #[test]
    #[ignore]
    fn insert_id_biger() {
        {
            let mut db = BinacleFile::create("test11.db", 5, 6, 28).unwrap();
            helper_insert2(&mut db, 0x123, 0x0, 100000000);            
        }

        let _ = remove_file("test11.db");
        let _ = remove_file("test11.meta");
    }

    #[test]
    fn insert_same_id_twice() {
        {
            let mut db = BinacleFile::create("test12.db", 5, 6, 28).unwrap();
            helper_insert(&mut db, 0x12345678, 0xAABBCCDD, 1);     
            helper_insert(&mut db, 0x12345678, 0xAABBCCDD, 1);        
        }

        let _ = remove_file("test12.db");
        let _ = remove_file("test12.meta");
    }

    #[test]
    fn open_twice_read() {
        {
            let _ = BinacleFile::create("test13.db", 5, 6, 28).unwrap();
        }
        {
            let _ = BinacleFile::open_read("test13.db").unwrap();
            let _ = BinacleFile::open_read("test13.db").unwrap();
        }

        let _ = remove_file("test13.db");
        let _ = remove_file("test13.meta");
    }

    #[test]
    #[ignore]
    #[allow(unused_variables)]
    // this test should wait forever
    fn open_twice_write() {
        {
            let db1 = BinacleFile::create("test14.db", 5, 6, 28).unwrap();
            let db2 = BinacleFile::create("test14.db", 5, 6, 28).unwrap();
        }

        let _ = remove_file("test14.db");
        let _ = remove_file("test14.meta");
    }

    #[test]
    #[should_panic]
    fn open_nonexisting() {
        let _ = BinacleFile::open_read("test15.db").unwrap();   
    }

    #[test]
    fn write_readonly() {

        {
            let _ = BinacleFile::create("test16.db", 5, 6, 28).unwrap();
        }
        {
            let mut db = BinacleFile::open_read("test16.db").unwrap();
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                let _ = db.insert_ngram(0x123, 0x11223344);
            }));
            assert!(result.is_err());
        }
        let _ = remove_file("test16.db");
        let _ = remove_file("test16.meta");        
    }

    #[test]
    fn pack_integers() {
        let (p, nb) = BinacleFile::pack_integer(127);
        let (u, _) = BinacleFile::unpack_integer(p);
        assert_eq!(u, 127);
        assert_eq!(nb, 1);

        let (p, nb) = BinacleFile::pack_integer(16383);
        let (u, _) = BinacleFile::unpack_integer(p);
        assert_eq!(u, 16383);
        assert_eq!(nb, 2);

        let (p, nb) = BinacleFile::pack_integer(2097151);
        let (u, _) = BinacleFile::unpack_integer(p);
        assert_eq!(u, 2097151);
        assert_eq!(nb, 3);

        let (p, nb) = BinacleFile::pack_integer(268435455);
        let (u, _) = BinacleFile::unpack_integer(p);
        assert_eq!(u, 268435455);
        assert_eq!(nb, 4);
    }

    #[test]
    fn list_packed() {
        {
            let mut db = BinacleFile::create("test17.db", 5, 6, 28).unwrap();
            let _ = db.insert_ngram(18, 0x11);  
            let _ = db.insert_ngram(20, 0x11);    
            let m = db.get_ids_by_ngram(0x11);

            assert_eq!(m.len(), 2);
            assert_eq!(m.contains(&18), true);
            assert_eq!(m.contains(&20), true);
        }

        let _ = remove_file("test17.db");
        let _ = remove_file("test17.meta");  

    }

}
