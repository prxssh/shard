package api

import (
	"io"
)

// Storer defines the contract for a storage backend in the distributed system.
//
// It abstracts away the details of the underlying file system (e.g., local
// disk, S3, HDFS), allowing workers to read input splits and persist final
// results agnostically.
type Storer interface {
	// OpenRead opens a file for reading at a specific byte range.
	// This is primarily used by Mappers to read their assigned "virtual split".
	//
	// Parameters:
	//   path   - The file path or object key.
	//   offset - The byte offset to start reading from.
	//   length - The number of bytes to read.
	//
	// Returns:
	//   A ReadCloser containing the data for the requested split.
	OpenRead(path string, offset, length int64) (io.ReadCloser, error)

	// OpenWrite opens a file for writing.
	// This is used by Reducers to write the final output of the job,
	// or by Mappers to write spill files (if using a shared DFS for shuffle).
	//
	// Parameters:
	//   path - The destination file path or object key.
	//
	// Returns:
	//   A WriteCloser that must be closed to flush data and ensure persistence.
	OpenWrite(path string) (io.WriteCloser, error)
}
