package master

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prxssh/shard/internal/task"
	"golang.org/x/sync/errgroup"
)

// Config holds the configuration parameters for the Master node.
type Config struct {
	// SplitSize is the size (in bytes) of each Map task input.
	SplitSize int64

	// NReduce is the number of Reduce tasks (and output partitions) to create.
	// This determines how many buckets Mappers should hash keys into.
	NReduce int

	// Addr is the TCP address (host:port) where the Master listens for RPC
	// connections.
	Addr string

	// OutputDir is the location where intermediate and final files are stored.
	OutputDir string

	// WorkerInactivityDuration is the duration after which a worker is
	// considered stalled or dead.
	WorkerInactivityDuration time.Duration
}

// Master is the central coordinator of the MapReduce job.
//
// It manages the lifecycle of all tasks, tracks worker status, and handles
// fault tolerance.
type Master struct {
	cfg    *Config
	logger *slog.Logger

	// mut guards the task lists and worker registry to ensure thread safety during
	// concurrent RPC calls from multiple workers.
	mut sync.RWMutex

	// mapTasks holds all Map tasks generated from the input files.
	mapTasks []*task.Task

	// reduceTasks holds all Reduce tasks, one for each partition (0 to NReduce-1).
	reduceTasks []*task.Task

	// mapping of workerID to its metadata
	workers map[uuid.UUID]*worker

	// metadata stats
	stats *stats
}

type worker struct {
	lastActivityAt time.Time
	task           *task.Task
	totalAssigned  atomic.Uint64
	completedTasks atomic.Uint64
	failedTasks    atomic.Uint64
}

type stats struct {
	completed   atomic.Uint64
	failed      atomic.Uint64
	progressing atomic.Uint64
}

// Start initializes the Master, generates all necessary tasks from the input
// files, and starts the RPC server to begin accepting Worker connections.
//
// It blocks until the server fails or is stopped.
func Start(inputFiles []string, cfg *Config, logger *slog.Logger) error {
	if cfg == nil {
		return errors.New("master: config can't be nil")
	}

	m := &Master{
		cfg:     cfg,
		logger:  logger,
		stats:   &stats{},
		workers: make(map[uuid.UUID]*worker),
	}
	if m.logger == nil {
		m.logger = slog.Default()
	}

	globalID := int64(1)

	for _, file := range inputFiles {
		newTasks, err := m.createMapTasks(file, &globalID)
		if err != nil {
			return fmt.Errorf("failed to generate map tasks for %s: %w", file, err)
		}
		m.mapTasks = append(m.mapTasks, newTasks...)
	}
	m.logger.Info("map tasks generated", "count", len(m.mapTasks))

	m.createReduceTasks(&globalID)
	m.logger.Info("reduce tasks generated", "count", len(m.reduceTasks))

	var grp errgroup.Group

	grp.Go(func() error { return m.serve() })
	grp.Go(func() error { return m.reclaimStalledWorkerLoop() })

	return grp.Wait()
}

func (m *Master) createMapTasks(inputFile string, globalID *int64) ([]*task.Task, error) {
	stat, err := os.Stat(inputFile)
	if err != nil {
		return nil, err
	}

	fileSize := stat.Size()
	numChunks := (fileSize + m.cfg.SplitSize - 1) / m.cfg.SplitSize
	tasks := make([]*task.Task, 0, numChunks)

	for i := int64(0); i < numChunks; i++ {
		offset := i * m.cfg.SplitSize
		length := min(m.cfg.SplitSize, fileSize-offset)

		tasks = append(tasks, &task.Task{
			ID:      *globalID,
			Type:    task.TypeMap,
			File:    inputFile,
			Offset:  offset,
			Len:     length,
			State:   task.StateIdle,
			NReduce: m.cfg.NReduce,
		})
		*globalID++
	}

	return tasks, nil
}

func (m *Master) createReduceTasks(globalID *int64) {
	for i := 0; i < m.cfg.NReduce; i++ {
		m.reduceTasks = append(m.reduceTasks, &task.Task{
			ID:       *globalID,
			Type:     task.TypeReduce,
			State:    task.StateIdle,
			NReduce:  m.cfg.NReduce,
			ReduceID: i,
		})
		*globalID++
	}
}

func (m *Master) serve() error {
	rpc.Register(m)
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", m.cfg.Addr)
	if err != nil {
		return err
	}

	m.logger.Info("master server started", "addr", m.cfg.Addr)
	return http.Serve(listener, nil)
}

func (m *Master) reclaimStalledWorkerLoop() error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		m.mut.Lock()
		now := time.Now()

		for id, w := range m.workers {
			if now.Sub(w.lastActivityAt) > m.cfg.WorkerInactivityDuration {
				m.logger.Warn("found dead worker", "id", id)

				if w.task.State == task.StateProgress {
					w.task.State = task.StateIdle
					w.task.WorkerID = uuid.Nil
					w.task.StartTime = time.Time{}
					w.task = nil

					m.stats.progressing.Add(^uint64(0))
					m.stats.failed.Add(1)
					w.failedTasks.Add(1)
				}
			}
		}
	}

	return nil
}

func (m *Master) scanTasks(tasks []*task.Task, workerID uuid.UUID) (*task.Task, bool) {
	allDone := true

	for _, t := range tasks {
		if t.State == task.StateIdle {
			t.State = task.StateProgress
			t.StartTime = time.Now()
			t.WorkerID = workerID

			w, exists := m.workers[workerID]
			if !exists {
				w = &worker{}
				m.workers[workerID] = w
			}
			w.task = t
			w.lastActivityAt = time.Now()
			w.totalAssigned.Add(1)

			m.stats.progressing.Add(1)

			return t, false
		}

		if t.State != task.StateCompleted {
			allDone = false
		}
	}

	return nil, allDone
}
