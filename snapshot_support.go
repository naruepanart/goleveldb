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
	db.mu.RLock()
	defer db.mu.RUnlock()

	var seq uint64
	if opts != nil && opts.Snapshot != nil {
		seq = opts.Snapshot.sequenceNumber
	} else {
		seq = db.sequenceNumber.Load()
	}

	// ค้นหาใน memtable ก่อน
	if db.mem != nil {
		if value, err := db.mem.get(key, seq); err == nil {
			return value, nil
		}
	}

	// ค้นหาใน immutable memtable
	if db.imm != nil {
		if value, err := db.imm.get(key, seq); err == nil {
			return value, nil
		}
	}

	// ค้นหาใน SSTables
	if value, err := db.getFromSSTables(key, seq); err == nil {
		return value, nil
	}

	return nil, ErrNotFound
}

func (m *memTable) get(key []byte, maxSeq uint64) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.table == nil {
		return nil, ErrNotFound
	}

	iter := m.table.iterator()
	iter.Seek(key)

	// หา entry ล่าสุดที่ match กับ key
	var foundValue []byte
	var foundSeq uint64
	var foundKind uint8

	for iter.Valid() {
		currentKey := iter.Key()
		decodedKey, seq, kind := decodeInternalKey(currentKey)

		if !bytes.Equal(decodedKey, key) {
			break // ไม่ใช่ key เดียวกัน
		}

		if seq <= maxSeq && seq > foundSeq {
			foundSeq = seq
			foundKind = kind
			foundValue = iter.Value()
		}

		iter.Next()
	}

	if foundSeq > 0 {
		if foundKind == typeDeletion {
			return nil, ErrNotFound
		}
		return foundValue, nil
	}

	return nil, ErrNotFound
}
