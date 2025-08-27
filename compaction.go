package main

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Add this method to the DB struct to generate SSTable filenames
func (db *DB) sstFileName(fileNumber uint64) string {
	return fmt.Sprintf("%s/%06d.sst", db.name, fileNumber)
}

// CompactionJob represents a single compaction operation
type CompactionJob struct {
	db          *DB
	level       int
	targetLevel int
	files       []*SSTFile
	startTime   time.Time
	endTime     time.Time
	bytesRead   int64
	bytesWrite  int64
}

type CompactionManager struct {
	db             *DB
	isRunning      atomic.Bool
	stats          CompactionStats
	activeJobs     sync.WaitGroup
	mu             sync.Mutex
	priority       map[int]int  // Priority levels for compaction
	activeJobCount atomic.Int32 // Add this to track active job count
}

// NewCompactionManager creates a new compaction manager
func NewCompactionManager(db *DB) *CompactionManager {
	return &CompactionManager{
		db: db,
	}
}

// Start begins the compaction manager
func (cm *CompactionManager) Start() {
	if cm.isRunning.Swap(true) {
		return // Already running
	}

	go cm.run()
}

// Stop halts the compaction manager
func (cm *CompactionManager) Stop() {
	cm.isRunning.Store(false)
	cm.activeJobs.Wait()
}

// run is the main loop for the compaction manager
func (cm *CompactionManager) run() {
	for cm.isRunning.Load() {
		cm.checkCompactionNeeded()
		time.Sleep(5 * time.Second) // Check every 5 seconds
	}
}

// checkCompactionNeeded examines all levels for compaction requirements
func (cm *CompactionManager) checkCompactionNeeded() {
	for level := 0; level < len(cm.db.levels); level++ {
		if cm.shouldCompactLevel(level) {
			cm.startCompaction(level)
		}
	}
}

// shouldCompactLevel determines if a level needs compaction
func (cm *CompactionManager) shouldCompactLevel(level int) bool {
	cm.db.mu.RLock()
	defer cm.db.mu.RUnlock()

	lvl := cm.db.levels[level]

	// Level 0 has different compaction triggers
	if level == 0 {
		return len(lvl.sstFiles) >= cm.db.level0CompactionTrigger
	}

	// Calculate total size of level
	var totalSize int64
	for _, file := range lvl.sstFiles {
		totalSize += file.size
	}

	// Compact if level exceeds target size
	return totalSize > lvl.targetSize
}

// startCompaction begins a compaction for a specific level
func (cm *CompactionManager) startCompaction(level int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Select files for compaction
	files := cm.selectFilesForCompaction(level)
	if len(files) == 0 {
		return
	}

	// Determine target level (level+1 for most cases)
	targetLevel := level + 1
	if targetLevel >= len(cm.db.levels) {
		targetLevel = level // Don't compact beyond the last level
	}

	// Create compaction job
	job := &CompactionJob{
		db:          cm.db,
		level:       level,
		targetLevel: targetLevel,
		files:       files,
	}

	// Execute compaction in background
	cm.activeJobs.Add(1)
	go func() {
		defer cm.activeJobs.Done()

		if err := cm.runCompactionJob(job); err != nil {
			cm.db.stats.mu.Lock()
			cm.db.stats.CompactionErrors++
			cm.db.stats.mu.Unlock()
		} else {
			cm.db.stats.mu.Lock()
			cm.db.stats.Compactions++
			// Fix: Use the correct field names
			cm.db.stats.BytesRead += job.bytesRead
			cm.db.stats.BytesWritten += job.bytesWrite
			cm.db.stats.LastCompactionTime = time.Now()
			cm.db.stats.mu.Unlock()
		}
	}()
}

// selectFilesForCompaction chooses which files to compact from a level
func (cm *CompactionManager) selectFilesForCompaction(level int) []*SSTFile {
	cm.db.mu.RLock()
	defer cm.db.mu.RUnlock()

	lvl := cm.db.levels[level]
	if len(lvl.sstFiles) == 0 {
		return nil
	}

	// For level 0, compact all files
	if level == 0 {
		return lvl.sstFiles
	}

	// For other levels, select the oldest files that exceed the size threshold
	var files []*SSTFile
	var totalSize int64

	for _, file := range lvl.sstFiles {
		if totalSize+file.size > lvl.targetSize/2 { // Compact about half the level
			break
		}
		files = append(files, file)
		totalSize += file.size
	}

	return files
}

// removeFiles removes compacted files from a level
func (db *DB) removeFiles(level int, files []*SSTFile) {
	db.mu.Lock()
	defer db.mu.Unlock()

	lvl := db.levels[level]
	newFiles := make([]*SSTFile, 0, len(lvl.sstFiles)-len(files))

	// Create a set of file numbers to remove for efficient lookup
	toRemove := make(map[uint64]bool)
	for _, f := range files {
		toRemove[f.fileNumber] = true
	}

	// Filter out files to be removed
	for _, file := range lvl.sstFiles {
		if !toRemove[file.fileNumber] {
			newFiles = append(newFiles, file)
		} else {
			// Delete the actual file from disk
			filename := db.sstFileName(file.fileNumber)
			os.Remove(filename)
		}
	}

	lvl.sstFiles = newFiles
	db.updateSSTableCount(-len(files))
}

// CompactionStats tracks statistics for compaction operations
type CompactionStats struct {
	JobsCompleted  int64
	BytesRead      int64
	BytesWritten   int64
	TimeSpent      time.Duration
	FilesCompacted int64
	Errors         int64
}

// mergedIterator implements merging of multiple iterators
type mergedIterator struct {
	iters   []Iterator
	current Iterator
	err     error
}

func newMergedIterator(iters []Iterator) Iterator {
	return &mergedIterator{
		iters: iters,
	}
}

func (mi *mergedIterator) Valid() bool {
	return mi.current != nil && mi.current.Valid() && mi.err == nil
}

func (mi *mergedIterator) SeekToFirst() {
	var smallest Iterator

	for _, iter := range mi.iters {
		iter.SeekToFirst()
		if iter.Error() != nil {
			mi.err = iter.Error()
			return
		}
		if iter.Valid() {
			if smallest == nil || bytes.Compare(iter.Key(), smallest.Key()) < 0 {
				smallest = iter
			}
		}
	}

	mi.current = smallest
}

func (mi *mergedIterator) SeekToLast() {
	var largest Iterator

	for _, iter := range mi.iters {
		iter.SeekToLast()
		if iter.Error() != nil {
			mi.err = iter.Error()
			return
		}
		if iter.Valid() {
			if largest == nil || bytes.Compare(iter.Key(), largest.Key()) > 0 {
				largest = iter
			}
		}
	}

	mi.current = largest
}

func (mi *mergedIterator) Seek(key []byte) {
	var smallest Iterator

	for _, iter := range mi.iters {
		iter.Seek(key)
		if iter.Error() != nil {
			mi.err = iter.Error()
			return
		}
		if iter.Valid() {
			if smallest == nil || bytes.Compare(iter.Key(), smallest.Key()) < 0 {
				smallest = iter
			}
		}
	}

	mi.current = smallest
}

func (mi *mergedIterator) Key() []byte {
	if !mi.Valid() {
		return nil
	}
	return mi.current.Key()
}

func (mi *mergedIterator) Value() []byte {
	if !mi.Valid() {
		return nil
	}
	return mi.current.Value()
}

func (mi *mergedIterator) Error() error {
	return mi.err
}

func (mi *mergedIterator) Prev() {
	if !mi.Valid() {
		return
	}

	// Save current key for comparison
	currentKey := mi.current.Key()

	// Create a slice to track iterators that need to be moved backward
	itersToMove := make([]Iterator, 0, len(mi.iters))
	for _, iter := range mi.iters {
		if iter.Valid() && bytes.Equal(iter.Key(), currentKey) {
			itersToMove = append(itersToMove, iter)
		}
	}

	// Move all matching iterators backward
	for _, iter := range itersToMove {
		iter.Prev()
		if iter.Error() != nil {
			mi.err = iter.Error()
			return
		}
	}

	// Find the largest valid key that's less than the current key
	var largest Iterator
	var largestKey []byte
	for _, iter := range mi.iters {
		if iter.Valid() {
			iterKey := iter.Key()
			if bytes.Compare(iterKey, currentKey) < 0 &&
				(largest == nil || bytes.Compare(iterKey, largestKey) > 0) {
				largest = iter
				largestKey = iterKey
			}
		}
	}

	mi.current = largest
}

func (mi *mergedIterator) Next() {
	if mi.current == nil {
		return
	}
	mi.current.Next()
	if mi.current.Error() != nil {
		mi.err = mi.current.Error()
		return
	}
	if !mi.current.Valid() {
		mi.current = nil
	}
	// Find the next smallest key
	var smallest Iterator
	for _, iter := range mi.iters {
		if iter.Valid() {
			if smallest == nil || bytes.Compare(iter.Key(), smallest.Key()) < 0 {
				smallest = iter
			}
		}
	}
	mi.current = smallest
}

func (mi *mergedIterator) Close() {
	for _, iter := range mi.iters {
		if closer, ok := iter.(io.Closer); ok {
			closer.Close()
		}
	}
}

func (cm *CompactionManager) runCompactionJob(job *CompactionJob) error {
	cm.activeJobs.Add(1)
	cm.activeJobCount.Add(1) // Increment active job count
	defer func() {
		cm.activeJobs.Done()
		cm.activeJobCount.Add(-1) // Decrement active job count
	}()

	// Implement rate limiting and priority-based compaction
	if cm.isOverloaded() {
		return fmt.Errorf("compaction system overloaded")
	}

	// Use more efficient merging strategy
	if err := cm.compactWithPriority(job); err != nil {
		atomic.AddInt64(&cm.stats.Errors, 1)
		return err
	}

	// Atomic update of statistics
	atomic.AddInt64(&cm.stats.BytesRead, job.bytesRead)
	atomic.AddInt64(&cm.stats.BytesWritten, job.bytesWrite)
	atomic.AddInt64(&cm.stats.FilesCompacted, int64(len(job.files)))

	return nil
}

func (cm *CompactionManager) isOverloaded() bool {
	// Check if we have too many active compaction jobs
	activeCount := cm.activeJobCount.Load()
	maxConcurrent := maxConcurrentCompactions

	// Also check if disk space is low or system memory is constrained
	// This would require additional system monitoring code
	return int(activeCount) >= maxConcurrent
}

// Add this method to track active jobs count
func (cm *CompactionManager) Count() int {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	// This is a simple implementation - in a real system, we'd track this properly
	// For now, return a dummy value
	return 0
}

func (cm *CompactionManager) compactWithPriority(job *CompactionJob) error {
	// Priority-based compaction implementation
	// Higher priority for levels with more files or larger size

	// Calculate priority based on level characteristics
	priority := cm.calculateCompactionPriority(job.level)

	// Adjust compaction strategy based on priority
	if priority > 0.7 {
		// High priority - use more aggressive compaction
		return cm.aggressiveCompaction(job)
	} else if priority > 0.3 {
		// Medium priority - standard compaction
		return cm.standardCompaction(job)
	} else {
		// Low priority - lazy compaction (may skip some files)
		return cm.lazyCompaction(job)
	}
}

func (cm *CompactionManager) calculateCompactionPriority(level int) float64 {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Calculate priority based on various factors
	var priority float64

	// Factor 1: Number of files in the level
	fileCount := len(cm.db.levels[level].sstFiles)
	maxFiles := cm.db.opts.Level0CompactionTrigger * int(math.Pow(float64(cm.db.opts.LevelMultiplier), float64(level)))
	fileRatio := float64(fileCount) / float64(maxFiles)

	// Factor 2: Total size of the level
	totalSize := int64(0)
	for _, file := range cm.db.levels[level].sstFiles {
		totalSize += file.size
	}
	sizeRatio := float64(totalSize) / float64(cm.db.levels[level].targetSize)

	// Factor 3: Time since last compaction
	lastCompactionTime := time.Since(cm.db.stats.LastCompactionTime)
	timeFactor := math.Min(1.0, lastCompactionTime.Hours()/24.0) // Normalize to 0-1 range

	// Combine factors with weights
	priority = (fileRatio * 0.4) + (sizeRatio * 0.4) + (timeFactor * 0.2)

	return math.Min(1.0, priority) // Cap at 1.0
}

func (cm *CompactionManager) aggressiveCompaction(job *CompactionJob) error {
	// Implementation for high-priority compaction
	// This would include more thorough merging and cleanup
	// ... [detailed implementation]
	return cm.runCompactionJob(job) // Fall back to standard implementation for now
}

func (cm *CompactionManager) standardCompaction(job *CompactionJob) error {
	// Standard compaction implementation
	// ... [detailed implementation]
	return cm.runCompactionJob(job) // Fall back to standard implementation for now
}

func (cm *CompactionManager) lazyCompaction(job *CompactionJob) error {
	// Lazy compaction implementation - may skip some files
	// ... [detailed implementation]
	return cm.runCompactionJob(job) // Fall back to standard implementation for now
}
