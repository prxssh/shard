package master

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/prxssh/shard/pkg/coretype"
)

type worker struct {
	id             string
	address        string
	status         coretype.Status
	lastActivityAt time.Time
}

type Master struct {
	addr   string
	logger *slog.Logger

	workerMut sync.RWMutex
	workers   map[string]*worker
}

func NewMaster(addr string, logger *slog.Logger) (*Master, error) {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("src", "master")

	return &Master{
		logger:  logger,
		addr:    addr,
		workers: make(map[string]*worker),
	}, nil
}

func (m *Master) Start() error {
	m.logger.Info("starting server", "addr", m.addr)

	rpcServer := rpc.NewServer()
	if err := rpcServer.Register(m); err != nil {
		return fmt.Errorf("failed to register RPC server: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)
	mux.Handle(rpc.DefaultDebugPath, rpcServer)

	listener, err := net.Listen("tcp", m.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", m.addr, err)
	}

	m.logger.Info("master is ready to accept workers")

	if err := http.Serve(listener, mux); err != nil {
		return fmt.Errorf("http serve failed: %w", err)
	}
	return nil
}

func (m *Master) RegisterWorker(
	param *coretype.RegisterWorkerArgs,
	reply *coretype.RegisterWorkerReply,
) error {
	if param == nil {
		reply.Success = false
		return errors.New("invalid or nil args for register worker")
	}

	m.workerMut.Lock()
	defer m.workerMut.Unlock()

	w, exists := m.workers[param.ID]
	if !exists {
		m.logger.Info("registering new worker", "id", param.ID, "addr", param.Addr)

		m.workers[param.ID] = &worker{
			id:             param.ID,
			address:        param.Addr,
			lastActivityAt: time.Now(),
		}
	} else {
		m.logger.Debug("worker already registered, skipping", "id", param.ID, "addr", param.Addr)
		w.lastActivityAt = time.Now()
		w.address = param.Addr
	}

	reply.Success = true
	return nil
}
