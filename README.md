# ArcaneDB

Might be a graph storage engine.

## TODO

- [x] PageStore interface and implementation based on leveldb
- [x] LogStore interface and implementation based on Env of leveldb
- [x] BufferPoolManager
  - [] Swap pages
- [x] Type Subsystem: Schema, Properties
- [x] Sortkey
- [x] row read/write
  - [] More row type
- [x] In-memory page read/write
  - [] More page type. i.e. COW page
- [x] 1-level Btr
- [x] Versioned Btr
- [x] Transaction component
  - [x] Implement Hekaton style occ
  - [] More concurrency control protocol
- [x] Graph interface
- [x] WAL
- [x] Recovery
- [x] Page serialization/deserialization
- [x] Flush dirty page
- [x] Support range scan
- [] 2-level Btr
- [] Multi-level Btr

# Dependency

Init git submodule

```
git submodule update --init --recursive
```

Jemalloc is required.

```
sudo apt install libjemalloc-dev
```

brpc dependency

```
sudo apt-get install -y git g++ make libssl-dev libgflags-dev libprotobuf-dev libprotoc-dev protobuf-compiler libleveldb-dev
```

