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

	"github.com/prxssh/shard/internal/task"
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
		cfg:    cfg,
		logger: logger,
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

	return m.serve()
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
