package api

import "hash/fnv"

// HashPartitioner is the default implementation using FNV-1a hashing.
// It guarantees that the same key always ends up on the same partition.
//
// Algorithm: FNV-1a_32(key) % numPartitions
func HashPartitioner(key string, numPartitions int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	hashValue := h.Sum32()

	// Masking with 0x7fffffff to zero out the sign bit.
	return int(hashValue&0x7fffffff) % numPartitions
}
