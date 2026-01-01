package shard

import (
	"runtime"
)

const (
	DefaultMasterAddress = "localhost:6969"
	DefaultOutputDir     = "./shard"
	DefaultNumReducers   = 16
	DefaultChunkSize     = 64 * 1024 * 1024 // 64MB
)

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
	Mapper any

	// Reducer is the client provided implementation of the Reduce function.
	Reducer any

	// Partitioner determines which reducer handles a specific key. If nil,
	// a default hash-based partitioner is typically applied.
	Partitioner any

	// Storere handles the abstraction of reading and writing files (e.g.,
	// wrapping local disk IO or cloud storage calls).
	Storer any
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

func WithMapper(mapper any) Option {
	return func(cfg *Config) {
		cfg.Mapper = mapper
	}
}

func WithReducer(reducer any) Option {
	return func(cfg *Config) {
		cfg.Reducer = reducer
	}
}

func WithPartitioner(partitioner any) Option {
	return func(cfg *Config) {
		cfg.Partitioner = partitioner
	}
}

func WithStorer(storer any) Option {
	return func(cfg *Config) {
		cfg.Storer = storer
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

	return cfg, nil
}

func (c *Config) normalize() error {
	return nil
}

func (c *Config) validate() error {
	return nil
}

func defaultConfig() *Config {
	return &Config{
		MasterAddress:  DefaultMasterAddress,
		OutputDir:      DefaultOutputDir,
		NumReducers:    DefaultNumReducers,
		ChunkSize:      DefaultChunkSize,
		MaxConcurrency: DefaultMaxConcurrency,
	}
}
