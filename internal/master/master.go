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
	"time"

	"github.com/prxssh/shard/internal/transport"
)

type TaskState uint8

const (
	StateIdle TaskState = iota
	StateExecuting
	StateCompleted
)

type TaskType uint8

const (
	TaskMap TaskType = iota
	TaskReduce
)

type Task struct {
	ID        int
	Type      TaskType
	Status    TaskState
	WorkerID  string
	StartTime time.Time
	InputFile string
	Offset    int64
}

type Master struct {
	logger     *slog.Logger
	listenPort string
	nReduce    int
	chunkSize  int
	files      []string

	mut        sync.RWMutex
	workers    map[string]*remoteWorker
	mapTask    []Task
	reduceTask []Task
	isDone     bool

	timeout time.Duration
}

type remoteWorker struct {
	id             string
	addr           string
	lastActivityAt time.Time
}

type options struct {
	port      int
	timeout   time.Duration
	logger    *slog.Logger
	chunkSize int
}

type Option func(*options)

func defaultOptions() *options {
	return &options{
		port:      1234,
		timeout:   10 * time.Second,
		logger:    slog.Default(),
		chunkSize: 64 * 1024 * 1024, // 64 MB
	}
}

func WithPort(port int) Option {
	return func(o *options) { o.port = port }
}

func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}

func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

func WithChunkSize(chunkSize int) Option {
	return func(o *options) { o.chunkSize = chunkSize }
}

func NewMaster(inputFiles []string, nReduce int, opts ...Option) (*Master, error) {
	if len(inputFiles) == 0 {
		return nil, errors.New("master: input files required")
	}

	if nReduce < 1 {
		return nil, errors.New("master: nReduce must be >= 1")
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	m := Master{
		logger:     o.logger.With("src", "master"),
		listenPort: fmt.Sprintf(":%d", o.port),
		nReduce:    nReduce,
		files:      inputFiles,
		workers:    make(map[string]*remoteWorker),
		timeout:    o.timeout,
	}

	taskID := 0

	for _, file := range inputFiles {
		info, err := os.Stat(file)
		if err != nil {
			m.logger.Error(
				"unable to get file stat",
				"error",
				err.Error(),
				"file",
				file,
			)
			return nil, err
		}

		fileSize := info.Size()
		chunkSize := int64(o.chunkSize)

		for offset := int64(0); offset < fileSize; offset += chunkSize {
			if offset+chunkSize > fileSize {
				chunkSize = fileSize - offset
			}

			task := Task{
				ID:        taskID,
				Type:      TaskMap,
				InputFile: file,
				Status:    StateIdle,
				Offset:    offset,
			}

			m.mapTask = append(m.mapTask, task)
			m.reduceTask = append(m.reduceTask, task)
			taskID++
		}
	}

	return &m, nil
}

func (m *Master) Start() error {
	m.logger.Debug("starting server", "addr", m.listenPort)

	rpcServer := rpc.NewServer()
	if err := rpcServer.Register(m); err != nil {
		return fmt.Errorf("master: register rpc server %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)
	mux.Handle(rpc.DefaultDebugPath, rpcServer)

	listener, err := net.Listen("tcp", m.listenPort)
	if err != nil {
		return fmt.Errorf("master: listener %w", err)
	}

	m.logger.Debug("master is up and running...")

	if err := http.Serve(listener, mux); err != nil {
		return fmt.Errorf("master: http serve %w", err)
	}
	return nil
}

func (m *Master) RegisterWorker(
	arg *transport.RegisterWorkerParam,
	reply *transport.RegisterWorkerReply,
) error {
	if arg == nil {
		reply.Success = false
		return errors.New("master: inavlid or nil args")
	}

	m.mut.Lock()
	defer m.mut.Unlock()

	worker, exists := m.workers[arg.ID]
	if !exists {
		m.workers[arg.ID] = &remoteWorker{
			addr:           arg.Addr,
			lastActivityAt: time.Now(),
		}

		m.logger.Debug("registered new worker", "id", arg.ID, "addr", arg.Addr)
	} else {
		worker.addr = arg.Addr
		worker.lastActivityAt = time.Now()

		m.logger.Debug("worker already registered; skipped", "id", arg.ID, "addr", arg.Addr)
	}

	reply.Success = true
	return nil
}
