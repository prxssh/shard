package worker

import (
	"container/heap"
	"encoding/gob"
	"io"

	"github.com/prxssh/shard/api"
)

type sliceIterator struct {
	values []string
	index  int
}

func (si *sliceIterator) Next() (string, bool) {
	if si.index >= len(si.values) {
		return "", false
	}
	val := si.values[si.index]
	si.index++
	return val, true
}

func (si *sliceIterator) Close() error {
	return nil
}

type mergeEntry struct {
	key       string
	value     string
	streamIdx int
}

type fileStream struct {
	closer  io.Closer
	decoder *gob.Decoder
}

type mergeIterator struct {
	streams []*fileStream
	pq      priorityQueue
	err     error
}

func newMergeIterator(filenames []string, fs api.Filesystem) (*mergeIterator, error) {
	mi := &mergeIterator{
		streams: make([]*fileStream, len(filenames)),
		pq:      make(priorityQueue, 0, len(filenames)),
	}

	success := false
	defer func() {
		if !success {
			mi.Close()
		}
	}()

	for i, name := range filenames {
		f, err := fs.Open(name)
		if err != nil {
			return nil, err
		}

		stream := &fileStream{
			closer:  f,
			decoder: gob.NewDecoder(f),
		}
		mi.streams[i] = stream

		var kv kvEntry
		if err := stream.decoder.Decode(&kv); err == nil {
			entry := &mergeEntry{
				key:       kv.Key,
				value:     kv.Value,
				streamIdx: i,
			}
			heap.Push(&mi.pq, entry)
		} else if err != io.EOF {
			return nil, err
		}
	}

	success = true
	return mi, nil
}

func (mi *mergeIterator) Next() (string, string, bool) {
	if mi.pq.Len() == 0 {
		return "", "", false
	}

	// 1. Pop the smallest item from the heap
	minEntry := heap.Pop(&mi.pq).(*mergeEntry)
	key, val := minEntry.key, minEntry.value

	// 2. Refill the heap from the exact same stream the item came from
	idx := minEntry.streamIdx
	stream := mi.streams[idx]

	var nextKV kvEntry
	if err := stream.decoder.Decode(&nextKV); err == nil {
		newEntry := &mergeEntry{
			key:       nextKV.Key,
			value:     nextKV.Value,
			streamIdx: idx,
		}
		heap.Push(&mi.pq, newEntry)
	} else if err != io.EOF {
		// If we hit a non-EOF error, we store it and stop.
		// In a real system, you might log this.
		mi.err = err
	}

	return key, val, true
}

func (mi *mergeIterator) Error() error {
	return mi.err
}

func (mi *mergeIterator) Close() error {
	var firstErr error

	for _, s := range mi.streams {
		if s != nil && s.closer != nil {
			if err := s.closer.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// --- Priority Queue Implementation (Min-Heap) ---

type priorityQueue []*mergeEntry

func (pq priorityQueue) Len() int { return len(pq) }

// Less compares keys to determine order.
// Uses string comparison: "apple" < "banana".
func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].key < pq[j].key
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue) Push(x interface{}) {
	entry := x.(*mergeEntry)
	*pq = append(*pq, entry)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil // Avoid memory leak
	*pq = old[0 : n-1]
	return entry
}
