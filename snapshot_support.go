package main

import (
	"bytes"
	"sync/atomic"
)

type Snapshot struct {
	sequenceNumber uint64
	refCount       int32
}

func (db *DB) GetSnapshot() *Snapshot {
	db.snapshotMu.Lock()
	defer db.snapshotMu.Unlock()

	seq := db.sequenceNumber.Load()
	snapshot := &Snapshot{
		sequenceNumber: seq,
		refCount:       1,
	}
	db.snapshots = append(db.snapshots, snapshot)
	db.snapshotRefs[seq] = snapshot
	return snapshot
}

func (db *DB) ReleaseSnapshot(snapshot *Snapshot) {
	db.snapshotMu.Lock()
	defer db.snapshotMu.Unlock()

	if atomic.AddInt32(&snapshot.refCount, -1) == 0 {
		for i, snap := range db.snapshots {
			if snap == snapshot {
				db.snapshots = append(db.snapshots[:i], db.snapshots[i+1:]...)
				break
			}
		}
	}
}

func (db *DB) getOldestSnapshot() uint64 {
	db.snapshotMu.Lock()
	defer db.snapshotMu.Unlock()
	if len(db.snapshotRefs) == 0 {
		return 0
	}
	oldest := uint64(1<<64 - 1) // Max uint64
	for _, snap := range db.snapshotRefs {
		if snap.sequenceNumber < oldest {
			oldest = snap.sequenceNumber
		}
	}
	return oldest
}

func (db *DB) Get(opts *ReadOptions, key []byte) ([]byte, error) {
	db.incrementReads()
	var seq uint64
	if opts != nil && opts.Snapshot != nil {
		seq = opts.Snapshot.sequenceNumber
	} else {
		seq = db.sequenceNumber.Load()
	}

	// Check memtable first
	if value, err := db.mem.get(key, seq); err == nil {
		return value, nil
	}

	// Check immutable memtable
	if db.imm != nil {
		if value, err := db.imm.get(key, seq); err == nil {
			return value, nil
		}
	}

	// Check SSTables
	return db.getFromSSTables(key, seq)
}

// In memTable.get, only return values visible to the specified sequence number.
func (m *memTable) get(key []byte, maxSeq uint64) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.buf.Reset()
	encodeInternalKey(m.buf, maxSeq, typeValue, key)
	searchKey := m.buf.Bytes()
	iter := m.table.iterator()
	iter.Seek(searchKey)
	for iter.Valid() {
		internalKey := iter.Key()
		userKey, seq, kind := decodeInternalKey(internalKey)
		if !bytes.Equal(userKey, key) {
			break
		}
		if seq <= maxSeq {
			if kind == typeDeletion {
				return nil, ErrNotFound
			}
			return iter.Value(), nil
		}
		iter.Next()
	}
	return nil, ErrNotFound
}
