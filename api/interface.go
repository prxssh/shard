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

// Storer abstracts the intermediate storage layer (the "Shuffle" phase).
//
// It is responsible for persisting data emitted by Mappers and serving it to
// Reducers. In local mode, this might write to an in-memory or local disk. In
// a distributed mode, this handles network transfers and distributed file
// systems.
type Storer interface {
	// Push writes a key-value pair to the intermediate storage.
	Push(key, value string) error

	// Pull returns an Iterator for a specific key.
	Pull(key string) (Iterator, error)

	// Keys returns a list of all unique keys currently in the store.
	Keys() ([]string, error)
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
