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
- [] 2-level Btr
- [] Transaction component
- [] Graph interface
- [] WAL
- [] Recovery
- [] Page serialization/deserialization
- [] Flush dirty page
- [] Multi-level Btr
