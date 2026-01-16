package worker

import "github.com/prxssh/shard/api"

type partitionBuffer struct {
	buckets map[int][]kvPair
}

type kvPair struct {
	key   string
	value string
}

func newPartitionBuffer(partitioner api.Partitioner, numPartitions int) *partitionBuffer {
	return &partitionBuffer{}
}

func (pb *partitionBuffer) insert(key, value string) {
}

func (pb *partitionBuffer) flush() error {
	return nil
}
