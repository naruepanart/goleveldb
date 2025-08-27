package main

import (
	"bytes"
	"errors"
)

type SSTableIterator struct {
	reader  *SSTableReader
	current int
	block   *blockReader
	err     error // Add error field
}

func (r *SSTableReader) NewIterator() *SSTableIterator {
	return &SSTableIterator{
		reader:  r,
		current: 0,
		err:     nil,
	}
}

// sstable_iterator.go
func (it *SSTableIterator) Seek(target []byte) {
	foundIndex := -1
	for i, entry := range it.reader.index {
		if bytes.Compare(entry.key, target) >= 0 {
			foundIndex = i
			break
		}
	}
	if foundIndex == -1 {
		it.current = -1
		it.block = nil
		return
	}
	it.current = foundIndex
	it.loadCurrentBlock() // This doesn't return a value
	if it.err != nil {    // Check error after loading block
		return
	}
	if !it.block.seekInBlock(target) {
		it.err = errors.New("seek in block failed")
	}
}

func (it *SSTableIterator) Next() {
	if it.block == nil {
		it.loadCurrentBlock()
		return
	}

	it.block.current++
	if !it.block.next() {
		// Current block exhausted, move to next block
		it.current++
		it.loadCurrentBlock()
	}
}

func (it *SSTableIterator) Valid() bool {
	return it.current < len(it.reader.index) && it.err == nil && it.block != nil && it.block.next()
}

func (it *SSTableIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	key, err := it.block.key()
	if err != nil {
		it.err = err
		return nil
	}
	return key
}

func (it *SSTableIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	value, err := it.block.value()
	if err != nil {
		it.err = err
		return nil
	}
	return value
}

func (it *SSTableIterator) Error() error {
	return it.err
}

func (it *SSTableIterator) loadCurrentBlock() {
	if it.current >= len(it.reader.index) || it.current < 0 {
		it.block = nil
		return
	}

	entry := it.reader.index[it.current]
	blockData, err := it.reader.readBlock(entry.offset, entry.size)
	if err != nil {
		it.err = err
		it.block = nil
		return
	}

	it.block = newBlockReader(blockData)
	it.block.current = 0 // Reset to beginning of block
}

// Additional methods required by the Iterator interface
func (it *SSTableIterator) SeekToFirst() {
	it.current = 0
	it.loadCurrentBlock()
}

func (it *SSTableIterator) SeekToLast() {
	if len(it.reader.index) == 0 {
		it.current = len(it.reader.index)
		return
	}

	// Start from last block
	it.current = len(it.reader.index) - 1
	it.loadCurrentBlock()

	if it.block == nil {
		return
	}

	// Seek to last entry in block
	it.block.offset = len(it.block.data) // Start from end
	it.block.prev()                      // Move to last entry
}

func (it *SSTableIterator) Prev() {
	if it.block == nil {
		// Try to load the last block if we don't have one
		if len(it.reader.index) == 0 {
			return
		}
		it.current = len(it.reader.index) - 1
		it.loadCurrentBlock()
		if it.block == nil {
			return
		}
		// Move to last entry in block
		it.block.current = len(it.block.restarts) - 1
		return
	}

	// Try to move backward within current block
	if it.block.prev() {
		return
	}

	// Move to previous block
	if it.current > 0 {
		it.current--
		it.loadCurrentBlock()
		if it.block != nil {
			// Position at last entry in new block
			it.block.current = len(it.block.restarts) - 1
		}
	} else {
		it.block = nil
	}
}

func (r *blockReader) prev() bool {
	if r.current > 0 {
		r.current--
		return true
	}
	return false
}
