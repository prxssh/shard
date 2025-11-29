package worker

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"github.com/google/uuid"
	"github.com/prxssh/shard/api"
	"github.com/prxssh/shard/internal/transport"
)

var (
	ErrMissingMapper  = errors.New("mapper function is required")
	ErrMissingReducer = errors.New("reducer function is required")
)

type Worker struct {
	logger         *slog.Logger
	masterAddr     string
	listenPort     string // ":8080" (where we bind)
	advertisedAddr string // "worker-1:8080" (what we tell master)
	id             string
	mapper         api.MapFunc
	reducer        api.ReduceFunc
}

type options struct {
	masterAddr     string
	port           int
	advertisedAddr string
	logger         *slog.Logger
}

type Option func(*options)

func defaultOptions() *options {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}

	return &options{
		masterAddr:     "localhost:1234",
		port:           8080,
		advertisedAddr: fmt.Sprintf("%s:8080", hostname),
		logger:         slog.Default(),
	}
}

func WithMasterAddr(addr string) Option {
	return func(o *options) { o.masterAddr = addr }
}

func WithPort(port int) Option {
	return func(o *options) { o.port = port }
}

func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

func WithAdvertisedAddr(addr string) Option {
	return func(o *options) { o.advertisedAddr = addr }
}

func NewWorker(mapper api.MapFunc, reducer api.ReduceFunc, opts ...Option) (*Worker, error) {
	if mapper == nil {
		return nil, ErrMissingMapper
	}
	if reducer == nil {
		return nil, ErrMissingReducer
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	id := uuid.NewString()
	logger := o.logger.With(slog.String("src", "worker"), slog.String("id", id))

	return &Worker{
		id:             id,
		logger:         logger,
		mapper:         mapper,
		reducer:        reducer,
		masterAddr:     o.masterAddr,
		listenPort:     fmt.Sprintf(":%d", o.port),
		advertisedAddr: o.advertisedAddr,
	}, nil
}

func (w *Worker) Start() error {
	listener, err := net.Listen("tcp", w.listenPort)
	if err != nil {
		w.logger.Error("failed to listen", "error", err.Error())
		return err
	}

	go func() {
		err := http.Serve(listener, nil)
		if err != nil {
			w.logger.Error("http serve failed", "error", err.Error())
		}
	}()

	if err := w.Register(); err != nil {
		return err
	}

	return w.pollTask()
}

func (w *Worker) Register() error {
	w.logger.Debug("attempting to register with master")

	client, err := rpc.DialHTTP("tcp", w.masterAddr)
	if err != nil {
		w.logger.Error(
			"unable to register worker; master dial failed",
			"error",
			err.Error(),
		)
		return err
	}
	defer client.Close()

	arg := &transport.RegisterWorkerParam{ID: w.id, Addr: w.advertisedAddr}
	var reply transport.RegisterWorkerReply

	if err = client.Call("Master.RegisterWorker", arg, &reply); err != nil {
		w.logger.Error("unable to register worker; rpc failed", "error", err.Error())
		return err
	}

	if !reply.Success {
		w.logger.Warn("unable to register worker; registration rpc denied")
		return errors.New("worker: registration rpc denied")
	}

	w.logger.Debug("worker registered with master")
	return nil
}

func (w *Worker) pollTask() error {
	select {}
	return nil
}
