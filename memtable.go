package main

import (
	"bytes"
	"encoding/binary"
	"sync"
)

type memTable struct {
	mu    sync.RWMutex
	table *skipList
	size  int
	buf   *bytes.Buffer
	db    *DB
}

const (
	typeDeletion = 0x00
	typeValue    = 0x01
)

func newMemTable(db *DB) *memTable {
	return &memTable{
		table: newSkipList(), // This creates a skipList but doesn't initialize head properly
		buf:   bytes.NewBuffer(nil),
		db:    db,
	}
}

func (m *memTable) put(seq uint64, kind uint8, key, value []byte, db *DB) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.buf.Reset()
	encodeInternalKey(m.buf, seq, kind, key)

	internalKey := make([]byte, m.buf.Len())
	copy(internalKey, m.buf.Bytes())

	valCopy := make([]byte, len(value))
	copy(valCopy, value)

	m.table.put(internalKey, valCopy)
	m.size += len(internalKey) + len(valCopy)

	// Update statistics if DB is provided
	if db != nil {
		db.updateMemtableSize(m.size)
	}
}

func encodeInternalKey(buf *bytes.Buffer, seq uint64, kind uint8, key []byte) {
	buf.Write(key)
	var seqBytes [8]byte
	binary.LittleEndian.PutUint64(seqBytes[:], (seq<<8)|uint64(kind))
	buf.Write(seqBytes[:])
}

func decodeInternalKey(ik []byte) ([]byte, uint64, uint8) {
	if len(ik) < 9 {
		return nil, 0, 0
	}
	n := len(ik)
	seq := binary.BigEndian.Uint64(ik[n-9 : n-1])
	kind := ik[n-1]
	key := ik[:n-9]
	return key, seq, kind
}

func (m *memTable) approximateMemoryUsage() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

func (it *memTableIterator) Prev() {
	if it.current == nil {
		it.SeekToLast()
		return
	}
	it.current = it.list.findLessThan(it.current.key)
	if it.current == it.list.head {
		it.current = nil
	}
}
