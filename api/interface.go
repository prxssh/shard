package api

import "io"

// Iterator represents a stream of values associated with a specific key.
//
// It allows the Reducer to process data larger than available RAM by strictly
// processing one item at a time.
type Iterator interface {
	// Next returns the next value in the stream.
	//
	// It returns false as the second return value when stream is exhausted.
	Next() (string, bool)

	// Close cleans up any resources (file handles, network connections)
	// associated with the iterator.
	io.Closer
}

// Filesystem abstracts the underlying storage layer, providing a unified API
// for interacting with local disks, distributed file systems (like HDFS), or
// object storage (like S3).
//
// Implementations of this interface must be safe for concurrent use by multiple
// goroutines.
type Filesystem interface {
	// Glob returns a list of file paths matching the specified pattern.
	//
	// The pattern syntax is implementation-specific but generally follows shell
	// globbing rules (e.g., "input/*.txt" or "s3://bucket/data/2023-*").
	Glob(pattern string) ([]string, error)

	// Size returns the size of the file in bytes.
	//
	// This is primarily used by the Master to calculate Input Splits (assigning
	// byte ranges to workers) without downloading the file content.
	Size(filename string) (int64, error)

	// Open opens the named file for reading.
	//
	// It returns an io.ReadSeekCloser, allowing the caller to efficiently Seek()
	// to a specific offset. This is crucial for workers to read only their
	// assigned partition (chunk) of a large input file.
	Open(filename string) (io.ReadSeekCloser, error)

	// Create opens the named file for writing.
	//
	// If the file already exists, it should be truncated. This is used by
	// workers to write intermediate shuffle data and final MapReduce outputs.
	Create(filename string) (io.WriteCloser, error)

	// Delete removes the named file from the storage system.
	//
	// This is used for cleaning up intermediate files after the Reduce phase
	// completes or for removing temporary artifacts upon job failure.
	Delete(filename string) error

	// Abs returns an absolute representation of the path.
	Abs(path string) (string, error)
}

// Emitter is the callback function passed to the Mapper.
//
// It allows the Mapper to "emit" intermediate key-value pairs.
type Emitter func(key, value string) error

// Mapper is a function that transforms raw input into intermediate key-value
// pairs.
//
// Arguments:
//   - key: The identifier for the input data (e.g., filename, offset).
//   - value: The raw data content (e.g., line of text, entire file).
//   - emit: A callback to output 0 or more key-value pairs.
//
// Returns:
//   - error: Any error encountered during aggregation.
type Mapper func(key, value string, emit Emitter) error

// Reducer is a function that aggregates values for a specific key.
//
// Arguments:
//   - key: The intermediate key (e.g., a word in WordCount).
//   - values: An Iterator allowing sequential access to all values for this
//     key.
//
// Returns:
//   - error: Any error encountered during aggregation.
type Reducer func(key string, values Iterator) error

// Partitioner determines which Reduce task should process a specific key.
//
// Arguments:
//   - key: The intermediate key to hash.
//   - numPartitions: The total number of available Reduce tasks.
//
// Returns:
//   - int: The partition index (0 to numPartitions-1).
type Partitioner func(key string, numPartitions int) int
