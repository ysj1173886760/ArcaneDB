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
- [] Graph interface
- [] 2-level Btr
- [] WAL
- [] Recovery
- [] Page serialization/deserialization
- [] Flush dirty page
- [] Multi-level Btr
