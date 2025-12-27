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
	// MapSplitSize is the size (in bytes) of each Map task input.
	MapSplitSize int64

	// ReducePartitions is the number of Reduce tasks to create. This
	// determines how many buckets Mappers should hash keys into.
	ReducePartitions int

	// listenAddr is the TCP address (host:port) where the Master listens.
	ListenAddr string

	// OutputDir is the location where intermediate and final files are stored.
	OutputDir string

	// WorkerTimeout is the duration after which a worker is considered stalled
	// or dead.
	WorkerTimeout time.Duration
}

// Master is the central coordinator of the MapReduce job.
type Master struct {
	cfg    *Config
	logger *slog.Logger

	// mu guards the task lists and worker registry to ensure thread safety
	// during concurrent RPC calls from multiple workers.
	mu sync.RWMutex

	// mapTasks holds all tasks for the Map phase.
	mapTasks []*task.Task

	// reduceTasks holds all tasks for the Reduce phase.
	reduceTasks []*task.Task

	// workers maps a WorkerID to its runtime metadata (status, current task).
	workers map[uuid.UUID]*worker

	// stats tracks global job progress metrics.
	stats *stats
}

// worker holds the runtime state of a connected worker node.
type worker struct {
	lastActivityAt time.Time
	currentTask    *task.Task
	assignedCount  atomic.Uint64
	completedCount atomic.Uint64
	failedCount    atomic.Uint64
}

// stats holds global counters for job progress.
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

	taskIDCounter := int64(1)

	for _, file := range inputFiles {
		newTasks, err := m.createMapTasks(file, &taskIDCounter)
		if err != nil {
			return fmt.Errorf("failed to generate map tasks for %s: %w", file, err)
		}
		m.mapTasks = append(m.mapTasks, newTasks...)
	}
	m.logger.Info("map tasks generated", "count", len(m.mapTasks))

	m.createReduceTasks(&taskIDCounter)
	m.logger.Info("reduce tasks generated", "count", len(m.reduceTasks))

	var grp errgroup.Group

	grp.Go(func() error { return m.serve() })
	grp.Go(func() error { return m.reclaimStalledWorkerLoop() })

	return grp.Wait()
}

// createMapTasks splits a single input file into multiple virtual Map tasks.
func (m *Master) createMapTasks(inputFile string, taskIDCounter *int64) ([]*task.Task, error) {
	stat, err := os.Stat(inputFile)
	if err != nil {
		return nil, err
	}

	fileSize := stat.Size()
	numChunks := (fileSize + m.cfg.MapSplitSize - 1) / m.cfg.MapSplitSize
	generatedTasks := make([]*task.Task, 0, numChunks)

	for i := int64(0); i < numChunks; i++ {
		offset := i * m.cfg.MapSplitSize
		length := min(m.cfg.MapSplitSize, fileSize-offset)

		generatedTasks = append(generatedTasks, &task.Task{
			ID:               *taskIDCounter,
			Type:             task.TypeMap,
			File:             inputFile,
			Offset:           offset,
			Len:              length,
			State:            task.StateIdle,
			ReducePartitions: m.cfg.ReducePartitions,
		})
		*taskIDCounter++
	}

	return generatedTasks, nil
}

// createReduceTasks initializes the Reduce tasks (one per partition).
func (m *Master) createReduceTasks(taskIDCounter *int64) {
	for i := 0; i < m.cfg.ReducePartitions; i++ {
		m.reduceTasks = append(m.reduceTasks, &task.Task{
			ID:               *taskIDCounter,
			Type:             task.TypeReduce,
			State:            task.StateIdle,
			ReducePartitions: m.cfg.ReducePartitions,
			ReduceID:         i,
		})
		*taskIDCounter++
	}
}

func (m *Master) serve() error {
	rpc.Register(m)
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", m.cfg.ListenAddr)
	if err != nil {
		return err
	}

	m.logger.Info("master server started", "addr", m.cfg.ListenAddr)
	return http.Serve(listener, nil)
}

func (m *Master) reclaimStalledWorkerLoop() error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		m.mu.Lock()
		now := time.Now()

		for id, w := range m.workers {
			if now.Sub(w.lastActivityAt) > m.cfg.WorkerTimeout {
				m.logger.Warn("worker detected dead", "worker-id", id)

				if w.currentTask != nil &&
					w.currentTask.State == task.StateProgress {
					w.currentTask.State = task.StateIdle
					w.currentTask.WorkerID = uuid.Nil
					w.currentTask.StartTime = time.Time{}
					w.currentTask = nil

					m.stats.progressing.Add(^uint64(0))
					m.stats.failed.Add(1)
					w.failedCount.Add(1)
				}
			}
		}
	}
	m.mu.Unlock()

	return nil
}

func (m *Master) findAndAssignTask(tasks []*task.Task, workerID uuid.UUID) (*task.Task, bool) {
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
			w.currentTask = t
			w.lastActivityAt = time.Now()
			w.assignedCount.Add(1)
			m.stats.progressing.Add(1)
			return t, false
		}

		if t.State != task.StateCompleted {
			allDone = false
		}
	}

	return nil, allDone
}
