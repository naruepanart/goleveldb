// leveldb.go
package main

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNotFound     = errors.New("key not found")
	ErrCorruption   = errors.New("data corruption")
	ErrNotSupported = errors.New("operation not supported")
	ErrClosed       = errors.New("database closed")
)

var DefaultOptions = Options{
	CreateIfMissing:             true,
	ErrorIfExists:               false,
	ParanoidChecks:              false,
	WriteBufferSize:             4 * 1024 * 1024, // 4MB
	MaxOpenFiles:                1000,
	BlockSize:                   4096,
	SyncWrites:                  false,
	MaxLevels:                   7,
	Level0CompactionTrigger:     4,
	Level0SlowdownWritesTrigger: 8,
	Level0StopWritesTrigger:     12,
	LevelMultiplier:             10,
	UseBloomFilter:              true,
	BloomFilterFPRate:           0.01,
}

type SSTFile struct {
	fileNumber uint64
	size       int64
	smallest   []byte
	largest    []byte
}

type Level struct {
	level      int
	sstFiles   []*SSTFile
	targetSize int64
}

type CompactionRange struct {
	StartKey []byte
	EndKey   []byte
	Level    int
}

type DB struct {
	mu             sync.RWMutex
	name           string
	opts           *Options
	mem            *memTable
	imm            *memTable
	manifestFile   *os.File
	logFile        *os.File
	logNumber      uint64
	nextFileNumber uint64
	sequenceNumber atomic.Uint64
	compactionCond *sync.Cond
	compactionMu   sync.Mutex
	closing        atomic.Bool
	compacting     atomic.Bool
	compactionWg   sync.WaitGroup
	writeStallMu   sync.Mutex
	writeStallCond *sync.Cond

	// Level-based compaction fields
	levels                      []*Level
	level0CompactionTrigger     int
	level0SlowdownWritesTrigger int
	level0StopWritesTrigger     int

	// Snapshot support
	snapshots    []*Snapshot
	snapshotMu   sync.Mutex
	snapshotRefs map[uint64]*Snapshot

	// Manual compaction
	manualCompactionChan chan *CompactionRange
	levelMu              []sync.RWMutex

	stats DBStats

	compactionStates    []*compactionState
	compactionSemaphore chan struct{}
	compactionStats     map[string]int64

	compactionSem chan struct{} // Semaphore for compaction concurrency
	readOnly      atomic.Bool   // Atomic read-only mode flag
}

type DBStats struct {
	mu                           sync.RWMutex
	Reads                        int64
	Writes                       int64
	Compactions                  int64
	MemtableSize                 int64
	SSTableCount                 int64
	LevelSizes                   []int64
	LastCompactionTime           time.Time
	BloomFilterHits              int64
	BloomFilterMisses            int64
	BloomFilterMemoryUsage       int64
	BloomFilterKeysAdded         int64
	BloomFilterTheoreticalFPRate float64
	BloomFilterActualFPRate      float64
	CompactionErrors             int64
	BloomFilterCount             int64
	BloomFilterTotalMemory       int64
	BloomFilterAverageFPRate     float64
	BytesRead                    int64
	BytesWritten                 int64
}

type Options struct {
	CreateIfMissing bool
	ErrorIfExists   bool
	ParanoidChecks  bool
	WriteBufferSize int
	MaxOpenFiles    int
	BlockSize       int
	SyncWrites      bool
	// Level-based compaction options
	MaxLevels                   int
	Level0CompactionTrigger     int
	Level0SlowdownWritesTrigger int
	Level0StopWritesTrigger     int
	LevelMultiplier             float64

	// Add bloom filter options
	UseBloomFilter    bool    // Enable/disable bloom filters
	BloomFilterFPRate float64 // False positive rate for bloom filters
}

type ReadOptions struct {
	VerifyChecksums bool
	FillCache       bool
	Snapshot        *Snapshot
}

type WriteOptions struct {
	Sync bool
}

type Batch struct {
	ops       []operation
	mu        sync.Mutex
	committed bool
}

type operation struct {
	key   []byte
	value []byte
	kind  uint8
}

// NewBatch creates a new write batch
func NewBatch() *Batch {
	return &Batch{
		ops: make([]operation, 0),
	}
}

func (b *Batch) Put(key, value []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.committed {
		return
	}

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	var valueCopy []byte
	if value != nil {
		valueCopy = make([]byte, len(value))
		copy(valueCopy, value)
	}

	b.ops = append(b.ops, operation{
		key:   keyCopy,
		value: valueCopy,
		kind:  typeValue,
	})
}

// Delete adds a delete operation to the batch
func (b *Batch) Delete(key []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.committed {
		return
	}

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	b.ops = append(b.ops, operation{
		key:   keyCopy,
		value: nil,
		kind:  typeDeletion,
	})
}

// Clear resets the batch, removing all operations
func (b *Batch) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.ops = b.ops[:0]
	b.committed = false
}

// Size returns the number of operations in the batch
func (b *Batch) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.ops)
}

// Add to leveldb.go to ensure the Write method is properly connected
func (db *DB) Write(opts *WriteOptions, batch *Batch) error {
	if db.readOnly.Load() {
		return errors.New("database in read-only mode")
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Implement write stall control based on compaction pressure
	if db.shouldStallWrites() {
		db.writeStallMu.Lock()
		db.writeStallCond.Wait()
		db.writeStallMu.Unlock()
	}

	return db.writeBatchInternal(opts, batch)
}

// leveldb.go - Add this method to the DB struct
func (db *DB) shouldStallWrites() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Check if we're already in a write stall
	if db.writeStallMu.TryLock() {
		db.writeStallMu.Unlock()
	} else {
		// Already in a write stall
		return true
	}

	// Check level 0 file count against thresholds
	level0Files := len(db.levels[0].sstFiles)
	if level0Files >= db.opts.Level0StopWritesTrigger {
		return true
	}

	// Check if compaction is behind
	if level0Files >= db.opts.Level0SlowdownWritesTrigger && db.compacting.Load() {
		return true
	}

	// Check memory usage
	memUsage := db.mem.approximateMemoryUsage()
	if memUsage >= db.opts.WriteBufferSize {
		return true
	}

	return false
}

func (db *DB) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	db.mu.RLock()
	defer db.mu.RUnlock()

	stats["memtable_size"] = db.mem.approximateMemoryUsage()
	stats["immutable_memtable"] = db.imm != nil

	// Add safety check for levels initialization
	if len(db.levels) > 0 {
		stats["sst_files_total"] = len(db.levels[0].sstFiles)

		levelStats := make([]map[string]interface{}, len(db.levels))
		for i, level := range db.levels {
			var totalSize int64
			for _, file := range level.sstFiles {
				totalSize += file.size
			}
			levelStats[i] = map[string]interface{}{
				"file_count":       len(level.sstFiles),
				"total_size":       totalSize,
				"target_size":      level.targetSize,
				"needs_compaction": totalSize > level.targetSize,
			}
		}
		stats["levels"] = levelStats
	} else {
		// Handle case where levels are not initialized yet
		stats["sst_files_total"] = 0
		stats["levels"] = []map[string]interface{}{}
	}

	// Read statistics from DBStats with proper locking
	db.stats.mu.RLock()
	defer db.stats.mu.RUnlock()

	stats["total_reads"] = db.stats.Reads
	stats["total_writes"] = db.stats.Writes
	stats["total_compactions"] = db.stats.Compactions
	stats["last_compaction_time"] = db.stats.LastCompactionTime

	// Bloom filter statistics
	stats["bloom_filter_hits"] = db.stats.BloomFilterHits
	stats["bloom_filter_misses"] = db.stats.BloomFilterMisses
	stats["bloom_filter_memory_usage"] = db.stats.BloomFilterMemoryUsage
	stats["bloom_filter_keys_added"] = db.stats.BloomFilterKeysAdded
	stats["bloom_filter_theoretical_fp_rate"] = db.stats.BloomFilterTheoreticalFPRate

	// Calculate actual false positive rate
	totalChecks := db.stats.BloomFilterHits + db.stats.BloomFilterMisses
	if totalChecks > 0 {
		stats["bloom_filter_actual_fp_rate"] = float64(db.stats.BloomFilterMisses) / float64(totalChecks)
	} else {
		stats["bloom_filter_actual_fp_rate"] = 0.0
	}

	// Add compaction statistics
	db.compactionMu.Lock()
	stats["compaction_states_count"] = len(db.compactionStates)
	stats["compaction_semaphore_available"] = len(db.compactionSemaphore)
	db.compactionMu.Unlock()

	return stats
}

func (db *DB) GetBloomFilterStats() map[string]interface{} {
	db.mu.RLock()
	defer db.mu.RUnlock()

	stats := make(map[string]interface{})
	totalFilters := 0
	totalMemory := int64(0)
	totalFPRate := 0.0

	// Check if levels are initialized before iterating
	if db.levels != nil {
		for level := 0; level < len(db.levels); level++ {
			for _, file := range db.levels[level].sstFiles {
				filename := fmt.Sprintf("%s/%06d.sst", db.name, file.fileNumber)
				reader, err := NewSSTableReader(filename)
				if err != nil {
					continue // Skip files that cannot be opened
				}

				if reader.HasBloomFilter() {
					totalFilters++
					filterStats := reader.BloomFilterStats()
					if filterStats != nil {
						if memoryUsage, ok := filterStats["memory_usage"].(int); ok {
							totalMemory += int64(memoryUsage)
						}
						if fpRate, ok := filterStats["false_positive_rate"].(float64); ok {
							totalFPRate += fpRate
						}
					}
				}
				reader.Close()
			}
		}
	}

	// Always include these fields with default values
	stats["total_filters"] = totalFilters
	stats["total_memory_bytes"] = totalMemory
	if totalFilters > 0 {
		stats["average_fp_rate"] = totalFPRate / float64(totalFilters)
		stats["memory_per_filter"] = totalMemory / int64(totalFilters)
	} else {
		stats["average_fp_rate"] = 0.0
		stats["memory_per_filter"] = int64(0)
	}

	return stats
}

func (db *DB) incrementReads() {
	db.stats.mu.Lock()
	defer db.stats.mu.Unlock()
	db.stats.Reads++
}

func (db *DB) incrementWrites() {
	db.stats.mu.Lock()
	defer db.stats.mu.Unlock()
	db.stats.Writes++
}

func (db *DB) incrementCompactions() {
	db.stats.mu.Lock()
	defer db.stats.mu.Unlock()
	db.stats.Compactions++
	db.stats.LastCompactionTime = time.Now()
}

func (db *DB) updateMemtableSize(size int) {
	db.stats.mu.Lock()
	defer db.stats.mu.Unlock()
	db.stats.MemtableSize = int64(size)
}

func (db *DB) updateSSTableCount(count int) {
	db.stats.mu.Lock()
	defer db.stats.mu.Unlock()
	db.stats.SSTableCount = int64(count)
}
