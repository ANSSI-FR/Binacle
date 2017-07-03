# Binacle
Indexation "full-bin" of binary files

Originally presented at [SSTIC 2017] ( https://www.sstic.org/2017/presentation/binacle_indexation_full-bin_de_fichiers_binaires/ )

# Warning
This is a beta version, use at your own risk!

# Technical aspects
Please read the [SSTIC 2017] ( https://www.sstic.org/2017/presentation/binacle_indexation_full-bin_de_fichiers_binaires/ ) white paper.

# Description
Binacle is a database written in Rust designed to index the full content of binary files. It splits hexadecimal sequences into 4-grams and inserts them in a hashtable file that is mapped in-memory.

# Compilation
*  Install Cargo, the Rust package manager
    
    On Ubuntu, Install Cargo with rustup:
    `curl https://sh.rustup.rs -sSf >> rust_install.sh`
    
    `chmod +x rust_install.sh`
    
    `./rust_install.sh`
    
    Add ~/.cargo/bin to PATH 

*  Launch "cargo build --release"

# Documentation
*  Create a new database:
./binacle -c <db_name> [map] <max_size> <alignment> <ngram_size>
ex: ./binacle -c testdb map 30000000000 6 28

* Insert a file in a database:
./binacle <db_name> -f <id> <file_path>

* Insert all files from a directory and the subdirectories:
./binacle <db_name> --rec <dir_path>
ex: ./binacle testdb --rec Windows_dir

* Search in the database:
./binacle <db_name> -s [hex] <string>
ex1: ./binacle testdb -s GetProcAddress
ex2: ./binacle testdb -s hex 28347654