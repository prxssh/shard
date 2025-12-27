package hash

import "hash/fnv"

func FNV(key string, nReduce int) int {
	h := fnv.New32a()
	h.Write([]byte(key))

	// mask with 0x7fffffff to ensure non-negative number before mod
	return int(h.Sum32()&0x7fffffff) % nReduce
}
