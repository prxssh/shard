package worker

import (
	"encoding/gob"
	"fmt"
	"path/filepath"

	"github.com/prxssh/shard/api"
)

type kvEntry struct {
	Key   string
	Value string
}

type partitionBuffer struct {
	buckets       map[int]map[string][]string
	numPartitions int
	partitioner   api.Partitioner
	combiner      api.Combiner
	fs            api.Filesystem
	taskID        uint64
	outputDir     string
}

func newPartitionBuffer(
	taskID uint64,
	partitioner api.Partitioner,
	combiner api.Combiner,
	numPartitions int,
	fs api.Filesystem,
	outputDir string,
) *partitionBuffer {
	return &partitionBuffer{
		taskID:        taskID,
		buckets:       make(map[int]map[string][]string),
		numPartitions: numPartitions,
		partitioner:   partitioner,
		combiner:      combiner,
		fs:            fs,
		outputDir:     outputDir,
	}
}

func (pb *partitionBuffer) insert(key, value string) {
	partitionIdx := pb.partitioner(key, pb.numPartitions)

	if _, exists := pb.buckets[partitionIdx]; !exists {
		pb.buckets[partitionIdx] = make(map[string][]string)
	}
	pb.buckets[partitionIdx][key] = append(pb.buckets[partitionIdx][key], value)
}

func (pb *partitionBuffer) flush() error {
	for partitionID, keyMap := range pb.buckets {
		filename := buildIntermediateFilename(partitionID, pb.taskID)
		path := filepath.Join(pb.outputDir, filename)

		file, err := pb.fs.Create(path)
		if err != nil {
			return err
		}

		encoder := gob.NewEncoder(file)
		fileEmitter := func(k, v string) error {
			return encoder.Encode(kvEntry{Key: k, Value: v})
		}

		for key, values := range keyMap {
			iter := &sliceIterator{values: values}

			if pb.combiner != nil {
				if err := pb.combiner(key, iter, fileEmitter); err != nil {
					file.Close()
					return err
				}
				continue
			}

			for _, v := range values {
				if err := fileEmitter(key, v); err != nil {
					file.Close()
					return err
				}
			}
		}
		file.Close()
		delete(pb.buckets, partitionID)
	}

	return nil
}

func buildIntermediateFilename(partitionID int, taskID uint64) string {
	return fmt.Sprintf("partition-%d-task-%d.shard", partitionID, taskID)
}
