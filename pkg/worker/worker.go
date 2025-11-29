package worker

import (
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"

	"github.com/google/uuid"
	"github.com/prxssh/shard/pkg/coretype"
)

type Config struct {
	MasterAddr string
	SourceAddr string
	Logger     *slog.Logger
}

func defaultConfig() *Config {
	return &Config{
		MasterAddr: ":1234",
		Logger:     slog.Default(),
	}
}

type Option func(cfg *Config)

func WithMasterAddr(addr string) Option {
	return func(cfg *Config) {
		cfg.MasterAddr = addr
	}
}

func WithSrcAddr(addr string) Option {
	return func(cfg *Config) {
		cfg.SourceAddr = addr
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(cfg *Config) {
		cfg.Logger = logger
	}
}

type Worker struct {
	id     string
	cfg    *Config
	logger *slog.Logger

	mapper  coretype.MapFunc
	reducer coretype.ReduceFunc
}

func NewWorker(
	mapper coretype.MapFunc,
	reducer coretype.ReduceFunc,
	opts ...Option,
) (*Worker, error) {
	if mapper == nil {
		return nil, errors.New("mapper is required")
	}
	if reducer == nil {
		return nil, errors.New("reducer is required")
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	workerID := uuid.NewString()
	logger := cfg.Logger.With("src", "worker").With("id", workerID)

	return &Worker{
		id:      workerID,
		cfg:     cfg,
		logger:  logger,
		mapper:  mapper,
		reducer: reducer,
	}, nil
}

func (w *Worker) Start() error {
	listener, err := net.Listen("tcp", w.cfg.SourceAddr)
	if err != nil {
		return err
	}

	w.logger.Info("worker listening on", "addr", w.cfg.SourceAddr)

	go func() {
		if err := http.Serve(listener, nil); err != nil {
			w.logger.Error("http serve failed", "error", err.Error())
		}
	}()

	if err := w.Register(); err != nil {
		return err
	}

	// TODO: remove blocking
	select {}
}

func (w *Worker) Register() error {
	w.logger.Info("attempting to register with master", "master_addr", w.cfg.MasterAddr)

	client, err := rpc.DialHTTP("tcp", w.cfg.MasterAddr)
	if err != nil {
		w.logger.Error("failed to dial master", "error", err.Error())
		return err
	}
	defer client.Close()

	args := &coretype.RegisterWorkerArgs{ID: w.id, Addr: w.cfg.SourceAddr}
	var reply coretype.RegisterWorkerReply

	err = client.Call("Master.RegisterWorker", args, &reply)
	if err != nil {
		w.logger.Error("registration RPC failed", "error", err.Error())
		return err
	}

	if !reply.Success {
		w.logger.Warn("master denied registration")
		return errors.New("registration rpc denied")
	}

	w.logger.Info("worker registered with master")
	return nil
}
