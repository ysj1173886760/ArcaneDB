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
- [] In-memory page read/write
- [] Page serialization/deserialization
- [] 1-level Btr
- [] 2-level Btr
- [] Multi-level Btr
- [] Transaction component
