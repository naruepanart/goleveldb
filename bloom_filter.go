package main

import (
	"encoding/binary"
	"errors"
	"hash"
	"hash/fnv"
	"math"
	"sync"

	"github.com/spaolacci/murmur3"
)

type BloomFilter struct {
	bits      []byte
	hashes    int
	bitSize   uint32
	mu        sync.RWMutex
	keyCount  uint32
	hashFuncs []hash.Hash64 // Multiple hash functions for better distribution
}

func NewBloomFilter(numEntries int, falsePositiveRate float64) *BloomFilter {
	bitSize := calculateOptimalBitSize(numEntries, falsePositiveRate)
	hashes := calculateOptimalNumHashes(numEntries, bitSize)

	return &BloomFilter{
		bits:      make([]byte, (bitSize+7)/8),
		hashes:    hashes,
		bitSize:   bitSize,
		hashFuncs: []hash.Hash64{murmur3.New64(), fnv.New64()},
	}
}

// calculateOptimalBitSize calculates the optimal number of bits
func calculateOptimalBitSize(n int, p float64) uint32 {
	if n <= 0 || p <= 0 || p >= 1 {
		return 1024
	}
	bitSize := float64(-n) * math.Log(p) / (math.Log(2) * math.Log(2))
	return uint32(math.Ceil(bitSize))
}

// calculateOptimalNumHashes calculates the optimal number of hash functions
func calculateOptimalNumHashes(n int, m uint32) int {
	if n <= 0 || m == 0 {
		return 4
	}
	k := float64(m) / float64(n) * math.Log(2)
	return int(math.Ceil(k))
}

func (bf *BloomFilter) Add(key []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	positions := bf.getBitPositions(key)
	for _, pos := range positions {
		bf.setBit(pos)
	}
	bf.keyCount++
}

func (bf *BloomFilter) getBitPositions(key []byte) []uint32 {
	positions := make([]uint32, bf.hashes)

	for i := 0; i < bf.hashes; i++ {
		bf.hashFuncs[i].Reset()
		bf.hashFuncs[i].Write(key)
		hashValue := bf.hashFuncs[i].Sum64()
		positions[i] = uint32(hashValue % uint64(bf.bitSize))
	}

	return positions
}

func (bf *BloomFilter) MayContain(key []byte) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	if bf.keyCount == 0 {
		return false
	}

	for _, pos := range bf.getBitPositions(key) {
		if !bf.getBit(pos) {
			return false
		}
	}
	return true
}

// AddString is a convenience method for string keys
func (bf *BloomFilter) AddString(key string) {
	bf.Add([]byte(key))
}

// MayContainString is a convenience method for string keys
func (bf *BloomFilter) MayContainString(key string) bool {
	return bf.MayContain([]byte(key))
}

// setBit sets a specific bit
func (bf *BloomFilter) setBit(position uint32) {
	index := position / 8
	bit := position % 8
	if index < uint32(len(bf.bits)) {
		bf.bits[index] |= 1 << bit
	}
}

// getBit gets a specific bit
func (bf *BloomFilter) getBit(position uint32) bool {
	index := position / 8
	bit := position % 8
	if index >= uint32(len(bf.bits)) {
		return false
	}
	return (bf.bits[index] & (1 << bit)) != 0
}

// Clear resets the Bloom filter
func (bf *BloomFilter) Clear() {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	for i := range bf.bits {
		bf.bits[i] = 0
	}
	bf.keyCount = 0
}

func (bf *BloomFilter) Size() uint32 {
	return bf.keyCount
}

func (bf *BloomFilter) Capacity() uint32 {
	return uint32(float64(bf.bitSize) / (-math.Log(0.01) / (math.Log(2) * math.Log(2))))
}

// FalsePositiveRate calculates the current false positive rate
func (bf *BloomFilter) FalsePositiveRate() float64 {
	if bf.keyCount == 0 {
		return 0.0
	}

	k := float64(bf.hashes)
	m := float64(bf.bitSize)
	n := float64(bf.keyCount)

	return math.Pow(1-math.Exp(-k*n/m), k)
}

// Merge combines another Bloom filter (must have same parameters)
func (bf *BloomFilter) Merge(other *BloomFilter) error {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	if bf.bitSize != other.bitSize || bf.hashes != other.hashes {
		return errors.New("bloom filters have different parameters")
	}

	other.mu.RLock()
	defer other.mu.RUnlock()

	for i := range bf.bits {
		bf.bits[i] |= other.bits[i]
	}

	bf.keyCount += other.keyCount
	return nil
}

func (bf *BloomFilter) Serialize() []byte {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	buf := make([]byte, 16+len(bf.bits))
	binary.LittleEndian.PutUint32(buf[0:4], bf.bitSize)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(bf.hashes))
	binary.LittleEndian.PutUint32(buf[8:12], bf.keyCount)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(bf.bits)))
	copy(buf[16:], bf.bits)
	return buf
}

func DeserializeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 16 {
		return nil, errors.New("invalid bloom filter data")
	}

	bitSize := binary.LittleEndian.Uint32(data[0:4])
	hashes := int(binary.LittleEndian.Uint32(data[4:8]))
	keyCount := binary.LittleEndian.Uint32(data[8:12])
	bitsLen := binary.LittleEndian.Uint32(data[12:16])

	if len(data) < 16+int(bitsLen) {
		return nil, errors.New("bloom filter data length mismatch")
	}

	bits := make([]byte, bitsLen)
	copy(bits, data[16:16+bitsLen])

	return &BloomFilter{
		bits:      bits,
		hashes:    hashes,
		bitSize:   bitSize,
		keyCount:  keyCount,
		hashFuncs: []hash.Hash64{fnv.New64()},
	}, nil
}

// MemoryUsage returns the memory usage in bytes
func (bf *BloomFilter) MemoryUsage() int {
	return len(bf.bits) + 20 // bits + overhead
}

// Optimize recalculates parameters based on current key count
func (bf *BloomFilter) Optimize(targetFalsePositiveRate float64) *BloomFilter {
	bf.mu.RLock()
	currentKeys := bf.keyCount
	bf.mu.RUnlock()

	if currentKeys == 0 {
		return bf
	}

	optimalBitSize := calculateOptimalBitSize(int(currentKeys), targetFalsePositiveRate)
	optimalHashes := calculateOptimalNumHashes(int(currentKeys), optimalBitSize)

	// Only recreate if significantly different
	if math.Abs(float64(optimalBitSize)-float64(bf.bitSize)) > float64(bf.bitSize)*0.2 ||
		math.Abs(float64(optimalHashes)-float64(bf.hashes)) > 2 {

		newBF := NewBloomFilter(int(currentKeys), targetFalsePositiveRate)
		// Note: Re-adding keys is not implemented here as Bloom filters don't store keys
		return newBF
	}

	return bf
}
