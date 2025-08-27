package main

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type blockReader struct {
	data     []byte
	restarts []uint32
	current  int
	offset   int
	err      error
}

func newBlockReader(data []byte) *blockReader {
	reader := &blockReader{data: data}
	reader.parseRestarts()
	return reader
}

func (r *blockReader) currentEntryOffset() int {
	if r.current >= len(r.restarts) {
		return -1
	}
	return int(r.restarts[r.current])
}

// Helper to decode a varint from a buffer and return value and new offset
func decodeVarint(buf []byte, offset int) (uint64, int, error) {
	val, n := binary.Uvarint(buf[offset:])
	if n <= 0 {
		return 0, offset, errors.New("invalid varint encoding")
	}
	return val, offset + n, nil
}

func (r *blockReader) parseRestarts() {
	if len(r.data) < 8 {
		r.restarts = []uint32{}
		return
	}

	// Get restart count from end of block
	restartCountOffset := len(r.data) - 4
	restartCount := int(binary.LittleEndian.Uint32(r.data[restartCountOffset:]))

	// Validate restart count
	maxPossible := (len(r.data) - 8) / 4
	if restartCount <= 0 || restartCount > maxPossible {
		r.restarts = []uint32{}
		return
	}

	// Parse restart points with bounds checking
	restartsStart := restartCountOffset - 4*restartCount
	if restartsStart < 0 {
		r.restarts = []uint32{}
		return
	}

	r.restarts = make([]uint32, restartCount)
	for i := 0; i < restartCount; i++ {
		pos := restartsStart + i*4
		if pos+4 <= len(r.data) {
			r.restarts[i] = binary.LittleEndian.Uint32(r.data[pos:])
		}
	}
}

func (r *blockReader) key() ([]byte, error) {
	offset := r.currentEntryOffset()
	if offset < 0 || offset >= len(r.data) {
		return nil, errors.New("invalid offset")
	}
	buf := r.data
	_, off, err := decodeVarint(buf, offset)
	if err != nil {
		return nil, err
	}
	nonShared, off, err := decodeVarint(buf, off)
	if err != nil {
		return nil, err
	}
	_, off, err = decodeVarint(buf, off)
	if err != nil {
		return nil, err
	}
	keySuffix := buf[off : off+int(nonShared)]

	return append([]byte{}, keySuffix...), nil
}

func (r *blockReader) next() bool {
	if r.offset >= len(r.data) {
		return false
	}

	// Read entry header
	_, off, err := decodeVarint(r.data, r.offset)
	if err != nil {
		return false
	}
	nonShared, off, err := decodeVarint(r.data, off)
	if err != nil {
		r.err = err
		return false
	}
	valueLen, off, err := decodeVarint(r.data, off)
	if err != nil {
		r.err = err
		return false
	}

	// Check bounds
	entryEnd := off + int(nonShared) + int(valueLen)
	if entryEnd > len(r.data) {
		r.err = errors.New("entry exceeds block bounds")
		return false
	}

	r.offset = entryEnd
	return true
}

func (r *blockReader) seekInBlock(target []byte) bool {
	r.current = 0
	for r.next() {
		key, err := r.key()
		if err != nil {
			return false
		}
		if bytes.Compare(key, target) >= 0 {
			return true
		}
		r.current++
	}
	return false
}

func (r *blockReader) value() ([]byte, error) {
	offset := r.currentEntryOffset()
	if offset < 0 || offset >= len(r.data) {
		return nil, errors.New("invalid offset")
	}
	buf := r.data
	_, off, err := decodeVarint(buf, offset)
	if err != nil {
		return nil, err
	}
	nonShared, off, err := decodeVarint(buf, off)
	if err != nil {
		return nil, err
	}
	valueLen, off, err := decodeVarint(buf, off)
	if err != nil {
		return nil, err
	}
	off += int(nonShared)
	val := buf[off : off+int(valueLen)]

	return append([]byte{}, val...), nil
}
