extern crate rustc_serialize;
extern crate docopt;
extern crate walkdir;
extern crate time;
extern crate regex;

use std::env;
use rustc_serialize::hex::FromHex;
use rustc_serialize::json;
use docopt::Docopt;
use regex::Regex;

mod binacle_manager;
mod binacle;

// Command line arguments are explained in readme

const USAGE: &'static str = "
Usage: 
       binacle -c <db_name> [map] <max_size> <alignment> <ngram_size>
       binacle <db_name> -f <id> <file>
       binacle <db_name> --files <files_and_ids>
       binacle <db_name> --rec <dir>
       binacle <db_name> -s [hex] <string>

Options:
    hex, --hex  Provide hexa string.
";

fn main() {

    let args = Docopt::new(USAGE)
                  .and_then(|d| d.argv(env::args()).parse())
                  .unwrap_or_else(|e| e.exit());


    if args.get_bool("-c") {
        let db_name = args.get_str("<db_name>");
        let is_map = args.get_bool("map");
        let max_size = args.get_str("<max_size>").parse::<u64>().unwrap();
        let alignment = args.get_str("<alignment>").parse::<u8>().unwrap();
        let ngram_size = args.get_str("<ngram_size>").parse::<u8>().unwrap();
        binacle_manager::BinacleManager::create(db_name, is_map, max_size, alignment, ngram_size).unwrap();
    }

    let mut db = binacle_manager::BinacleManager::open(args.get_str("<db_name>")).unwrap();

    if args.get_bool("-f") {
        let id = args.get_str("<id>").parse::<u32>().unwrap();
        let file = args.get_str("<file>");
        db.insert_file(file, id, true).unwrap();
    }

    else if args.get_bool("--rec") {
        let dir = args.get_str("<dir>");
        db.insert_dir_recursive(dir).unwrap();
    }

    else if args.get_bool("--files") {
        let files = args.get_str("<files_and_ids>");
        let re = Regex::new(r"(\d{1,10}) ([\w:\\ \._]+)").unwrap();

        for cap in re.captures_iter(files) {

            let id = cap[1].parse::<u32>().unwrap();
            let file = &cap[2];

            let res = db.insert_file(file, id, false);
            if let Err(e) = res {
                println!("{}:\"Error {}\"", id, e);
            }
        }
    }

    else if args.get_bool("-s") {

        let result_id = if args.get_bool("hex") {
            let pattern = args.get_str("<string>").from_hex().unwrap();
            db.search(&pattern).unwrap()

        } else {
            let pattern = args.get_str("<string>").as_bytes();
            db.search(pattern).unwrap()
        };

        if db.is_map() {
            let res = db.to_map(&result_id).unwrap();
            for f in &res {
                println!("{}", f);
            }
            println!("{} result(s)", res.len());
        } else {
            println!("{}", json::encode(&result_id).unwrap());    
        }
    }

}