package worker

import (
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"github.com/prxssh/shard/api"
	"golang.org/x/sync/errgroup"
)

// Config holds the runtime configuration and user-defined functions for a
// Worker.
type Config struct {
	// MasterAddr is the TCP address (host:port) of the Master node.
	// The worker acts as a client and dials this address to fetch tasks.
	MasterAddr string

	// Partitioner is a function that maps a key to a partition index.
	// If nil, a default hash-based partitioner is usually applied.
	Partitioner api.PartitionFunc

	// Reducer is the user-defined Reduce function.
	// It is called once for each unique key in the reduce phase.
	Reducer api.ReduceFunc

	// Mapper is the user-defined Map function.
	// It is called for each input split assigned to this worker.
	Mapper api.MapFunc

	// Combiner is an optional optimization function (Map-side Reduce).
	// If provided, it aggregates data locally before writing to storage,
	// reducing network I/O.
	Combiner api.ReduceFunc

	// OutputDir is the prefix or directory path where this worker will write
	// intermediate and final output files via the Storer.
	OutputDir string
}

// Worker represents a compute node in the cluster.
// It is stateless (persisting data only to the Storer) and fault-tolerant.
type Worker struct {
	cfg    *Config
	logger *slog.Logger

	// id is a unique identifier generated at startup.
	id uuid.UUID

	// fs is the abstraction for the underlying storage system (Local, S3,
	// HDFS). The worker uses this to read input splits and write results.
	fs api.Storer
}

func Start(fs api.Storer, cfg *Config, logger *slog.Logger) error {
	if cfg == nil {
		return errors.New("worker: config can't be nil")
	}

	if fs == nil {
		return errors.New("worker: fs is required")
	}

	w := &Worker{
		cfg:    cfg,
		logger: logger,
		id:     uuid.New(),
		fs:     fs,
	}
	if logger == nil {
		w.logger = slog.Default()
	}

	var grp errgroup.Group

	grp.Go(func() error { return w.workLoop() })
	grp.Go(func() error { return w.heartbeatLoop() })
	return grp.Wait()
}

func (w *Worker) workLoop() error {
	return nil
}

func (w *Worker) heartbeatLoop() error {
	return nil
}
