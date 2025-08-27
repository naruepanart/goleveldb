package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

type SSTableReader struct {
	file   *os.File
	index  []indexEntry
	footer sstFooter
	filter *BloomFilter
}

type sstFooter struct {
	indexOffset  uint64
	indexSize    uint64
	fileNumber   uint64
	filterOffset uint64
	filterSize   uint64
}

func NewSSTableReader(filename string) (*SSTableReader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	reader := &SSTableReader{file: file}
	if err := reader.readFooter(); err != nil {
		file.Close()
		return nil, err
	}

	if err := reader.readIndex(); err != nil {
		file.Close()
		return nil, err
	}

	if err := reader.readFilter(); err != nil {
		file.Close()
		return nil, err
	}

	return reader, nil
}

func (r *SSTableReader) readFooter() error {
	// Seek to footer position (last 64 bytes for enhanced footer)
	if _, err := r.file.Seek(-64, io.SeekEnd); err != nil {
		return err
	}

	footerData := make([]byte, 64)
	if _, err := io.ReadFull(r.file, footerData); err != nil {
		return err
	}

	r.footer.indexOffset = binary.LittleEndian.Uint64(footerData[0:8])
	r.footer.indexSize = binary.LittleEndian.Uint64(footerData[8:16])
	r.footer.fileNumber = binary.LittleEndian.Uint64(footerData[16:24])
	r.footer.filterOffset = binary.LittleEndian.Uint64(footerData[24:32])
	r.footer.filterSize = binary.LittleEndian.Uint64(footerData[32:40])

	// Verify checksum
	calculated := crc32.ChecksumIEEE(footerData[:60])
	stored := binary.LittleEndian.Uint32(footerData[60:64])
	if calculated != stored {
		return ErrCorruption
	}

	return nil
}

func (r *SSTableReader) readIndex() error {
	if _, err := r.file.Seek(int64(r.footer.indexOffset), io.SeekStart); err != nil {
		return err
	}

	indexData := make([]byte, r.footer.indexSize)
	if _, err := io.ReadFull(r.file, indexData); err != nil {
		return err
	}

	// Parse index blocks using block reader
	blockReader := newBlockReader(indexData)
	r.index = make([]indexEntry, 0)

	for blockReader.next() {
		key, err := blockReader.key()
		if err != nil {
			return fmt.Errorf("failed to read index key: %w", err)
		}
		value, err := blockReader.value()
		if err != nil {
			return fmt.Errorf("failed to read index value: %w", err)
		}

		if len(value) >= 16 {
			entry := indexEntry{
				key:    make([]byte, len(key)),
				offset: binary.LittleEndian.Uint64(value[0:8]),
				size:   binary.LittleEndian.Uint64(value[8:16]),
			}
			copy(entry.key, key)
			r.index = append(r.index, entry)
		}
	}

	return nil
}

func (r *SSTableReader) readFilter() error {
	if r.footer.filterSize == 0 {
		r.filter = nil
		return nil
	}

	if _, err := r.file.Seek(int64(r.footer.filterOffset), io.SeekStart); err != nil {
		return err
	}

	filterData := make([]byte, r.footer.filterSize)
	if _, err := io.ReadFull(r.file, filterData); err != nil {
		return err
	}

	bf, err := DeserializeBloomFilter(filterData)
	if err != nil {
		return fmt.Errorf("failed to deserialize bloom filter: %w", err)
	}
	r.filter = bf
	return nil
}

func (r *SSTableReader) deserializeBloomFilter(data []byte) (*BloomFilter, error) {
	bf, err := DeserializeBloomFilter(data)
	if err != nil {
		return nil, err
	}
	return bf, nil
}

func (r *SSTableReader) readBlock(offset, size uint64) ([]byte, error) {
	if _, err := r.file.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	blockData := make([]byte, size)
	if _, err := io.ReadFull(r.file, blockData); err != nil {
		return nil, err
	}

	// Verify block checksum if present
	if len(blockData) >= 4 {
		data := blockData[:len(blockData)-4]
		storedChecksum := binary.LittleEndian.Uint32(blockData[len(blockData)-4:])
		calculatedChecksum := crc32.ChecksumIEEE(data)

		if calculatedChecksum != storedChecksum {
			return nil, ErrCorruption
		}
		return data, nil
	}

	return blockData, nil
}

func (r *SSTableReader) Get(key []byte) ([]byte, error) {
	// Check bloom filter first
	if r.filter != nil && !r.filter.MayContain(key) {
		return nil, ErrNotFound
	}

	// Find appropriate block using binary search
	blockIndex := r.findBlockIndex(key)
	if blockIndex >= len(r.index) {
		return nil, ErrNotFound
	}

	targetBlock := r.index[blockIndex]

	// Read the block
	blockData, err := r.readBlock(targetBlock.offset, targetBlock.size)
	if err != nil {
		return nil, err
	}

	// Search within block with sequence number awareness
	blockReader := newBlockReader(blockData)
	for blockReader.next() {
		currentKey, err := blockReader.key()
		if err != nil {
			return nil, fmt.Errorf("failed to read key: %w", err)
		}

		userKey, _, kind := decodeInternalKey(currentKey)
		if bytes.Equal(userKey, key) {
			if kind == typeDeletion {
				return nil, ErrNotFound
			}
			value, err := blockReader.value()
			if err != nil {
				return nil, fmt.Errorf("failed to read value: %w", err)
			}
			return value, nil
		}
		if bytes.Compare(userKey, key) > 0 {
			break
		}
	}

	return nil, ErrNotFound
}

func (r *SSTableReader) Close() error {
	if r.file != nil {
		err := r.file.Close()
		r.file = nil
		return err
	}
	return nil
}

func (r *SSTableReader) findBlockIndex(key []byte) int {
	// Binary search in index to find the right block
	low, high := 0, len(r.index)-1

	for low <= high {
		mid := (low + high) / 2
		cmp := bytes.Compare(key, r.index[mid].key)

		if cmp == 0 {
			return mid
		} else if cmp < 0 {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	// Return the block that might contain the key
	if low > 0 {
		return low - 1
	}
	return 0
}

func (r *SSTableReader) searchInBlock(blockData, key []byte) ([]byte, error) {
	blockReader := newBlockReader(blockData)

	for blockReader.next() {
		currentKey, err := blockReader.key()
		if err != nil {
			return nil, fmt.Errorf("failed to read key: %w", err)
		}
		currentValue, err := blockReader.value()
		if err != nil {
			return nil, fmt.Errorf("failed to read value: %w", err)
		}

		cmp := bytes.Compare(key, currentKey)
		if cmp == 0 {
			return currentValue, nil
		} else if cmp < 0 {
			break // Key not found in this block
		}
	}

	return nil, ErrNotFound
}

func (r *SSTableReader) GetKeyRange() ([]byte, []byte) {
	if len(r.index) == 0 {
		return nil, nil
	}

	smallest := r.index[0].key
	largest := r.index[len(r.index)-1].key

	return smallest, largest
}

func (r *SSTableReader) HasBloomFilter() bool {
	return r.footer.filterSize > 0
}

func (r *SSTableReader) BloomFilterStats() map[string]interface{} {
	if r.filter == nil {
		return nil
	}

	stats := make(map[string]interface{})
	stats["memory_usage"] = r.filter.MemoryUsage()
	stats["false_positive_rate"] = r.filter.FalsePositiveRate()
	stats["key_count"] = r.filter.Size()
	stats["capacity"] = r.filter.Capacity()

	return stats
}
