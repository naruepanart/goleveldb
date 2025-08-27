package main

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	maxHeight = 12
	branching = 4
)

type skipList struct {
	head   *node
	height int32
	mu     sync.RWMutex
}

type node struct {
	key   []byte
	value []byte
	next  []unsafe.Pointer
}

func newSkipList() *skipList {
	head := &node{
		key:   []byte{},
		value: []byte{},
		next:  make([]unsafe.Pointer, maxHeight),
	}
	return &skipList{
		head:   head,
		height: 1,
		mu:     sync.RWMutex{},
	}
}

func (s *skipList) put(key, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	nk := make([]byte, len(key))
	copy(nk, key)
	nv := make([]byte, len(value))
	if value != nil {
		copy(nv, value)
	}

	prev := make([]*node, maxHeight)
	x := s.findGreaterOrEqual(key, prev)

	if x != nil && bytesEqual(x.key, nk) {
		x.value = nv
		return
	}

	height := randomHeight()
	if height > int(s.height) {
		for i := int(s.height); i < height; i++ {
			prev[i] = s.head
		}
		atomic.StoreInt32(&s.height, int32(height))
	}

	x = &node{
		key:   nk,
		value: nv,
		next:  make([]unsafe.Pointer, height),
	}

	for i := 0; i < height; i++ {
		x.next[i] = prev[i].next[i]
		prev[i].next[i] = unsafe.Pointer(x)
	}
}

func randomHeight() int {
	height := 1
	for height < maxHeight && rand.Intn(branching) == 0 {
		height++
	}
	return height
}

// Helper function to safely compare bytes with nil handling
func bytesEqual(a, b []byte) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return bytes.Equal(a, b)
}

func (s *skipList) iterator() *memTableIterator {
	return &memTableIterator{list: s}
}

func bytesCompare(a, b []byte) int {
	// Handle nil cases
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Compare the raw bytes directly
	return bytes.Compare(a, b)
}

func (s *skipList) findGreaterOrEqual(key []byte, prev []*node) *node {
	x := s.head
	level := int(s.height) - 1
	for {
		next := (*node)(atomic.LoadPointer(&x.next[level]))
		if next != nil && bytesCompare(next.key, key) < 0 {
			x = next
		} else {
			if prev != nil {
				prev[level] = x
			}
			if level == 0 {
				return next
			}
			level--
		}
	}
}

func (s *skipList) findLessThan(key []byte) *node {
	x := s.head
	level := int(s.height) - 1
	for level >= 0 {
		next := (*node)(atomic.LoadPointer(&x.next[level]))
		if next == nil || bytesCompare(next.key, key) >= 0 {
			level--
		} else {
			x = next
		}
	}
	return x
}
