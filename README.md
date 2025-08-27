# goleveldb

rm -rf ./testdb && go build . && ./goleveldb.exe

rm -rf ./test.log && go test -v > test.log 2>&1

repomix --ignore ".gitignore, .gitattributes" --no-file-summary --compress --remove-comments --remove-empty-lines

-----

A LevelDB-like key-value store implementation in Go.

## Features
- In-memory memtable with skip list
- Write-ahead logging (WAL)
- SSTable file format
- Basic compaction mechanism
- Level-based compaction structure
- Snapshot Support
- Manual Compaction
- Batch Writes
- Bloom Filters
- Basic Statistics

## Planned Features from Google LevelDB
- [ ] Block Cache - LRU cache for frequently accessed data blocks
- [ ] Advanced Metrics - Detailed performance statistics and histograms
- [ ] Enhanced Checksum Verification - Comprehensive data integrity checking
- [ ] Sophisticated Compaction - Adaptive compaction strategies
- [ ] Cache Warming - Pre-load frequently accessed data on startup
- [ ] Multi-threaded Compactions - Parallel compaction operations
- [ ] Compression Support - Snappy/Zstd compression for SSTables
- [ ] Backup & Restore - Database backup functionality
- [ ] Incremental Compaction - Background compaction without stalling writes
- [ ] Custom Comparators - Support for custom key ordering
