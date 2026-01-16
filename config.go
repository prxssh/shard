package shard

import (
	"fmt"
	"os"
	"runtime"

	"github.com/prxssh/shard/api"
)

const (
	DefaultMasterAddress = "localhost:6969"
	DefaultOutputDir     = "./shard"
	DefaultNumReducers   = 16
	DefaultChunkSize     = 64 * 1024 * 1024 // 64MB
)

// DefaultMaxConcurrency is the maximum number of tasks that a worker can
// execute concurrently.
var DefaultMaxConcurrency = runtime.NumCPU() * 2

// Config holds the runtime configuration for the Shard library. It defines
// infrastructure settings and the core processing logic.
type Config struct {
	// MasterAddress is the RPC address (host:port) of the coordinator service
	// where worker will register.
	//
	// If not specified, it defaults to DefaultMasterAddress.
	MasterAddress string

	// InputPath specifies the file or directory pattern (glob) to be processed
	// by the Map phase.
	InputPath string

	// OutputDir is the directory where intermediate files and final Reduce
	// outputs will be stored.
	//
	// If not specified, it defaults to DefaultOutputDir.
	OutputDir string

	// NumReducers is the number of reduce tasks (R). This determines the
	// number of output partitions.
	//
	// If not specified, it default to DefaultNumReducers.
	NumReducers int

	// ChunkSize is the maximum size in bytes for a single input split given
	// to a Mapper.
	//
	// If not specified, it defaults to DefaultChunkSize.
	ChunkSize int64

	// MaxConcurrency limits the number of concurrent tasks that can be
	// processed by a single worker.
	//
	// If not specified, it defaults to DefaultMaxConcurrency.
	MaxConcurrency int

	// Mapper is the client provided implementation of the Map function.
	Mapper api.Mapper

	// Reducer is the client provided implementation of the Reduce function.
	Reducer api.Reducer

	// Partitioner determines which reducer handles a specific key. If nil,
	// a default hash-based partitioner is typically applied.
	Partitioner api.Partitioner

	// Filesystem handles the abstraction of reading and writing files (e.g.,
	// wrapping local disk IO or cloud storage calls).
	Filesystem api.Filesystem

	// Logger is an interface that the logger (e.g., slog, zlog) should satisfy.
	Logger api.LoggerAdapter

	inputFiles []string
}

type Option func(*Config)

func WithMasterAddress(addr string) Option {
	return func(cfg *Config) {
		cfg.MasterAddress = addr
	}
}

func WithInputPath(path string) Option {
	return func(cfg *Config) {
		cfg.InputPath = path
	}
}

func WithOutputDir(path string) Option {
	return func(cfg *Config) {
		cfg.OutputDir = path
	}
}

func WithNumReducers(partitions int) Option {
	return func(cfg *Config) {
		cfg.NumReducers = partitions
	}
}

func WithMapSplitSize(size int64) Option {
	return func(cfg *Config) {
		cfg.ChunkSize = size
	}
}

func WithMaxConcurrency(limit int) Option {
	return func(cfg *Config) {
		cfg.MaxConcurrency = limit
	}
}

func WithMapper(mapper api.Mapper) Option {
	return func(cfg *Config) {
		cfg.Mapper = mapper
	}
}

func WithReducer(reducer api.Reducer) Option {
	return func(cfg *Config) {
		cfg.Reducer = reducer
	}
}

func WithPartitioner(partitioner api.Partitioner) Option {
	return func(cfg *Config) {
		cfg.Partitioner = partitioner
	}
}

func WithFilesystem(storer api.Filesystem) Option {
	return func(cfg *Config) {
		cfg.Filesystem = storer
	}
}

func WithLogger(logger api.LoggerAdapter) Option {
	return func(cfg *Config) {
		cfg.Logger = logger
	}
}

func NewConfig(opts ...Option) (*Config, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := cfg.normalize(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) normalize() error {
	var err error

	c.OutputDir, err = c.Filesystem.Abs(c.OutputDir)
	if err != nil {
		return fmt.Errorf("failed to resolve absolute output path: %w", err)
	}
	if err := os.MkdirAll(c.OutputDir, 0o755); err != nil {
		return fmt.Errorf("failed to create output directory %s: %w", c.OutputDir, err)
	}

	c.inputFiles, err = c.Filesystem.Glob(c.InputPath)
	if err != nil {
		return fmt.Errorf("failed to resolve input path: %w", err)
	}

	return nil
}

func (c *Config) validate() error {
	if c.InputPath == "" {
		return fmt.Errorf("input path is required")
	}
	if c.Filesystem == nil {
		return fmt.Errorf("filesystem is required")
	}
	if c.Mapper == nil {
		return fmt.Errorf("mapper is required")
	}
	if c.Reducer == nil {
		return fmt.Errorf("reducer is required")
	}
	if c.NumReducers < 1 {
		return fmt.Errorf("num reducers must be > 0, got %d", c.NumReducers)
	}

	return nil
}

func defaultConfig() *Config {
	return &Config{
		MasterAddress:  DefaultMasterAddress,
		OutputDir:      DefaultOutputDir,
		NumReducers:    DefaultNumReducers,
		ChunkSize:      DefaultChunkSize,
		MaxConcurrency: DefaultMaxConcurrency,
		Logger:         NewSlogLogger(nil),
		Partitioner:    api.HashPartitioner,
	}
}
