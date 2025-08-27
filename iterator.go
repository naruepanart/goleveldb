// iterator.go
package main

type Iterator interface {
	Valid() bool
	SeekToFirst()
	SeekToLast()
	Seek(target []byte)
	Next()
	Prev() // Make sure this is included
	Key() []byte
	Value() []byte
	Error() error
}

type memTableIterator struct {
	list    *skipList
	current *node
}

func (it *memTableIterator) Seek(target []byte) {
	it.current = it.list.findGreaterOrEqual(target, nil)
}

func (it *memTableIterator) Error() error {
	return nil
}

func (it *memTableIterator) SeekToFirst() {
	it.current = (*node)(it.list.head.next[0])
}

func (it *memTableIterator) Valid() bool {
	return it.current != nil && it.current.next[0] != nil
}

func (it *memTableIterator) Next() {
	if it.current != nil {
		it.current = (*node)(it.current.next[0])
	}
}

func (it *memTableIterator) Key() []byte {
	if it.current != nil {
		return it.current.key
	}
	return nil
}

func (it *memTableIterator) Value() []byte {
	if it.current != nil {
		return it.current.value
	}
	return nil
}

func (it *memTableIterator) SeekToLast() {
	if it.list == nil || it.list.head == nil {
		it.current = nil
		return
	}

	level := int(it.list.height) - 1
	x := it.list.head
	for level >= 0 {
		next := (*node)(x.next[level])
		for next != nil {
			x = next
			next = (*node)(x.next[level])
		}
		level--
	}

	if x != it.list.head {
		it.current = x
	} else {
		it.current = nil
	}
}
