package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/prxssh/shard/api"
	"github.com/prxssh/shard/internal/task"
	"golang.org/x/sync/errgroup"
)

const (
	hearbeatInterval = 5 * time.Second
	pollInterval     = 1 * time.Second
	retryInterval    = 5 * time.Second
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var grp errgroup.Group

	grp.Go(func() error {
		defer cancel()
		return w.workLoop(ctx)
	})

	grp.Go(func() error { return w.heartbeatLoop(ctx) })

	return grp.Wait()
}

func (w *Worker) workLoop(ctx context.Context) error {
	logger := w.logger.With("worker", w.id, "type", "work loop")

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		work, err := w.requestWork()
		if err != nil {
			logger.ErrorContext(
				ctx,
				"master unreachable, failed to request work...",
				"error", err,
			)

			if err := w.wait(ctx, retryInterval); err != nil {
				return nil
			}
			continue
		}

		switch work.Type {
		case task.TypeMap:
			err := w.doMap(work.Task)
			w.report(work.Task.ID, work.Type, err)
		case task.TypeReduce:
			err := w.doReduce(work.Task)
			w.report(work.Task.ID, work.Type, err)
		case task.TypeWait:
			if err := w.wait(ctx, pollInterval); err != nil {
				return nil
			}
		default:
			return nil
		}
	}
}

func (w *Worker) heartbeatLoop(ctx context.Context) error {
	ticker := time.NewTicker(hearbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.DebugContext(
				ctx,
				"exiting heartbeat loop, ctx canceled",
				"worker", w.id,
			)
			return nil

		case <-ticker.C:
			if err := w.pingMaster(); err != nil {
				w.logger.ErrorContext(ctx,
					"failed to ping master",
					"error", err,
					"worker", w.id,
				)
			}
		}
	}
}

func (w *Worker) wait(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Worker) doMap(task *task.Task) error {
	return nil
}

func (w *Worker) doReduce(task *task.Task) error {
	return nil
}
