package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	blockSize = 32768
)

var (
	ErrBlockFull  = errors.New("block is full")
	ErrEmptyBlock = errors.New("block is empty")
)

type blockBuilder struct {
	buffer   []byte
	restarts []uint32
	counter  int
	lastKey  []byte
}

func newBlockBuilder() *blockBuilder {
	return &blockBuilder{
		restarts: []uint32{0},
		counter:  0,
	}
}

func commonPrefix(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return minLen
}

func encodeEntry(buffer []byte, shared, nonShared, valueLen int, keySuffix, value []byte) ([]byte, error) {
	// Check if we have enough capacity
	requiredSize := binary.MaxVarintLen32*3 + nonShared + valueLen
	if cap(buffer)-len(buffer) < requiredSize {
		return nil, errors.New("buffer too small")
	}

	var buf [binary.MaxVarintLen32 * 3]byte
	n := binary.PutUvarint(buf[:], uint64(shared))
	n += binary.PutUvarint(buf[n:], uint64(nonShared))
	n += binary.PutUvarint(buf[n:], uint64(valueLen))

	buffer = append(buffer, buf[:n]...)
	buffer = append(buffer, keySuffix...)
	buffer = append(buffer, value...)

	return buffer, nil
}

func (b *blockBuilder) finish() ([]byte, error) {
	if len(b.buffer) == 0 {
		return nil, ErrEmptyBlock
	}

	// Add restarts with proper encoding
	offset := len(b.buffer)
	for _, restart := range b.restarts {
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], restart)
		b.buffer = append(b.buffer, buf[:]...)
	}

	// Add restart count
	var countBuf [4]byte
	binary.LittleEndian.PutUint32(countBuf[:], uint32(len(b.restarts)))
	b.buffer = append(b.buffer, countBuf[:]...)

	// Calculate checksum on the restarts section only
	checksumData := b.buffer[offset:]
	checksum := crc32.ChecksumIEEE(checksumData)

	var checksumBuf [4]byte
	binary.LittleEndian.PutUint32(checksumBuf[:], checksum)
	b.buffer = append(b.buffer, checksumBuf[:]...)

	return b.buffer, nil
}

func (b *blockBuilder) add(key, value []byte) error {
	shared := 0
	if b.counter < len(b.restarts) && bytes.Equal(b.lastKey, key) {
		shared = len(b.lastKey)
	}

	nonShared := len(key) - shared
	valueLen := len(value)

	// Check if block has space
	entrySize := binary.MaxVarintLen32*3 + nonShared + valueLen
	if len(b.buffer)+entrySize > blockSize {
		return ErrBlockFull
	}

	var err error
	b.buffer, err = encodeEntry(b.buffer, shared, nonShared, valueLen,
		key[shared:], value)
	if err != nil {
		return err
	}

	b.lastKey = append(b.lastKey[:0], key...)
	b.counter++

	if b.counter%15 == 0 { // Every 15 entries
		b.restarts = append(b.restarts, uint32(len(b.buffer)))
	}

	return nil
}

func (b *blockBuilder) size() int {
	return len(b.buffer)
}

func (b *blockBuilder) totalSize() int {
	size := len(b.buffer)
	// เพิ่มขนาดของ restarts array (ประมาณการ)
	size += len(b.restarts) * 4 // แต่ละ restart ใช้ 4 bytes
	return size
}

func decodeEntry(data []byte) (shared, nonShared, valueLen int, keySuffix, value []byte, err error) {
	if len(data) < binary.MaxVarintLen32*3 {
		return 0, 0, 0, nil, nil, errors.New("data too short")
	}

	var off int
	var val uint64

	val, off, err = decodeVarint(data, 0)
	if err != nil {
		return 0, 0, 0, nil, nil, err
	}
	shared = int(val)

	val, off, err = decodeVarint(data, off)
	if err != nil {
		return 0, 0, 0, nil, nil, err
	}
	nonShared = int(val)

	val, off, err = decodeVarint(data, off)
	if err != nil {
		return 0, 0, 0, nil, nil, err
	}
	valueLen = int(val)

	if off+nonShared+valueLen > len(data) {
		return 0, 0, 0, nil, nil, errors.New("invalid entry length")
	}

	keySuffix = data[off : off+nonShared]
	off += nonShared
	value = data[off : off+valueLen]

	return shared, nonShared, valueLen, keySuffix, value, nil
}

func searchInBlock(blockData, key []byte) ([]byte, error) {
	reader := newBlockReader(blockData)
	for reader.next() {
		currentKey, err := reader.key()
		if err != nil {
			return nil, fmt.Errorf("failed to read key: %w", err)
		}
		if bytes.Equal(currentKey, key) {
			value, err := reader.value()
			if err != nil {
				return nil, fmt.Errorf("failed to read value: %w", err)
			}
			return value, nil
		}
		if bytes.Compare(currentKey, key) > 0 {
			break
		}
	}
	return nil, ErrNotFound
}
