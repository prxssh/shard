package shard

import (
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/prxssh/shard/api"
	"github.com/prxssh/shard/pkg/hash"
)

const (
	// DefaultHTTPPort is the default port for the Master's status dashboard.
	DefaultHTTPPort = ":6969"

	// DefaultOutputDir is the default local directory for output files.
	DefaultOutputDir = "shard-output"

	// DefaultMapSplitSize is 64MB.
	DefaultMapSplitSize = 64 * 1024 * 1024
)

// Role defines the operational mode of the binary (Coordinator vs Executor).
type Role string

const (
	// RoleMaster indicates this instance is the coordinator.
	// It manages scheduling, fault tolerance, and the HTTP dashboard.
	RoleMaster Role = "master"

	// RoleWorker indicates this instance is a task executor.
	// It polls the Master for tasks and executes Map/Reduce logic.
	RoleWorker Role = "worker"
)

// Config holds the complete configuration for a MapReduce job.
type Config struct {
	// MasterAddr is the connection string (e.g., "localhost:6969").
	// The Master listens on this address; Workers dial it.
	MasterAddr string

	// Role determines if this process runs as Master or Worker.
	Role Role

	// HTTPPort is the port for the Master's status dashboard (e.g., ":8080").
	// Ignored if Role is RoleWorker.
	HTTPPort string

	// LogLevel sets the minimum severity of logs to emit (Debug, Info, Warn, Error).
	// Defaults to slog.LevelInfo.
	LogLevel slog.Level

	// -------------------------------------------------------------------------
	// User-Defined Functions (UDFs)
	// -------------------------------------------------------------------------

	// Mapper is the user's Map function (Required).
	Mapper api.MapFunc

	// Reducer is the user's Reduce function (Required).
	Reducer api.ReduceFunc

	// Combiner is an optional optimization (Map-side Reduce).
	// If set, it pre-aggregates data locally to reduce shuffle traffic.
	Combiner api.ReduceFunc

	// Partitioner determines which Reduce task a key belongs to.
	// Defaults to FNV-1a hash if nil.
	Partitioner api.PartitionFunc

	// -------------------------------------------------------------------------
	// Job Tuning & Infrastructure
	// -------------------------------------------------------------------------

	// ReducePartitions is the number of output partitions (R).
	ReducePartitions int

	// MapSplitSize is the size (in bytes) of each input split.
	// Defaults to 64MB.
	MapSplitSize int64

	// OutputDir is the prefix/directory for final output files.
	OutputDir string

	// InputFiles is the list of raw files to process.
	InputFiles []string

	// Storer abstracts the underlying storage (Local, S3, HDFS).
	Storer api.Storer

	// WorkerTimeout is the duration after which a silent worker is considered dead.
	WorkerTimeout time.Duration
}

type Option func(*Config)

func WithMasterAddr(addr string) Option {
	return func(c *Config) {
		c.MasterAddr = addr
	}
}

func WithRole(role Role) Option {
	return func(c *Config) {
		c.Role = role
	}
}

func WithMapper(fn api.MapFunc) Option {
	return func(c *Config) {
		c.Mapper = fn
	}
}

func WithReducer(fn api.ReduceFunc) Option {
	return func(c *Config) {
		c.Reducer = fn
	}
}

func WithCombiner(fn api.ReduceFunc) Option {
	return func(c *Config) {
		c.Combiner = fn
	}
}

func WithPartitioner(fn api.PartitionFunc) Option {
	return func(c *Config) {
		c.Partitioner = fn
	}
}

func WithReducePartitions(n int) Option {
	return func(c *Config) {
		c.ReducePartitions = n
	}
}

func WithMapSplitSize(size int64) Option {
	return func(c *Config) {
		c.MapSplitSize = size
	}
}

func WithHTTPPort(port string) Option {
	return func(c *Config) {
		c.HTTPPort = port
	}
}

func WithOutputDir(dir string) Option {
	return func(c *Config) {
		c.OutputDir = dir
	}
}

func WithInputGlob(pattern string) Option {
	return func(c *Config) {
		if pattern == "" {
			return
		}
		files, err := filepath.Glob(pattern)
		if err != nil {
			slog.Error("invalid glob pattern", "pattern", pattern, "error", err)
			return
		}
		if len(files) == 0 {
			slog.Warn("glob pattern matched no files", "pattern", pattern)
		}
		c.InputFiles = append(c.InputFiles, files...)
	}
}

func WithStorer(s api.Storer) Option {
	return func(c *Config) {
		c.Storer = s
	}
}

func WithWorkerTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.WorkerTimeout = d
	}
}

func WithLogLevel(level slog.Level) Option {
	return func(c *Config) {
		c.LogLevel = level
	}
}

func defaultConfig() *Config {
	return &Config{
		MapSplitSize:     DefaultMapSplitSize,
		HTTPPort:         DefaultHTTPPort,
		OutputDir:        DefaultOutputDir,
		ReducePartitions: 16,
		Partitioner:      hash.FNV,
		WorkerTimeout:    10 * time.Second,
		LogLevel:         slog.LevelInfo,
	}
}

func NewConfig(opts ...Option) *Config {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	if absPath, err := filepath.Abs(cfg.OutputDir); err == nil {
		cfg.OutputDir = absPath
	} else {
		slog.Warn("failed to resolve absolute path for output", "dir", cfg.OutputDir, "error", err)
	}

	return cfg
}

func (c *Config) validate() error {
	if c.Role != RoleMaster && c.Role != RoleWorker {
		return fmt.Errorf("invalid role: %s", c.Role)
	}

	if c.MasterAddr == "" {
		return errors.New("master address is required")
	}

	if c.Mapper == nil {
		return errors.New("mapper function is required")
	}

	if c.Reducer == nil {
		return errors.New("reducer function is required")
	}

	if c.ReducePartitions <= 0 {
		return errors.New("reduce partitions must be > 0")
	}

	if c.MapSplitSize <= 0 {
		return errors.New("map split size must be > 0")
	}

	if c.Storer == nil {
		return errors.New("storer backend is required")
	}

	return nil
}
