package shard

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/prxssh/shard/api"
)

const (
	defaultPort      = ":6969"
	defaultOutputDir = "~/.shard/"
)

// Role represents the operational mode of the Shard instance.
//
// A single binary can behave as either a Master (coordinator) or a Worker
// based on the this flag.
type Role string

const (
	// RoleMaster indicates this instance acts as the coordinator.
	// It manages task scheduling, failure detection, and the status dashboard.
	RoleMaster Role = "master"

	// RoleWorker indicates this instance acts as a task executor.
	// It polls the Master for tasks and execute the Map/Reduce/Combiner logic.
	RoleWorker Role = "worker"
)

// Config holds the infrastructure settings and user-defined logic for
// MapReduce job.
type Config struct {
	// MasterAddr is the connection string (e.g., "localhost:6969") for the
	// Master. Workers uses this to connect via RPC. The Master listens on this
	// address.
	MasterAddr string

	// Role determines the runtime behaviour of this process.
	Role Role

	// Mapper is the user's Map function (required).
	Mapper api.MapFunc

	// Reducer is the user's Reduce function (required).
	Reducer api.ReduceFunc

	// Combiner is an optional optimization function running on the Map worker.
	// It pre-aggregates data locally to reduce network traffic during the
	// shuffle. If nil, no combination is performed.
	Combiner api.ReduceFunc

	// Partitioner is an optional function to determine which Reduce task a key
	// belongs to. If nil, Shard uses a deafult FNV-1a hash of the key modulo
	// ReduceTasks.

	Partitioner api.PartitionFunc

	// ReduceTasks is the number of the output partitions (R).
	ReduceTasks int

	// DashboardPort is the port of the HTTP status page (e.g., ":6969").
	// Only used if the Role is RoleMaster. If nil, the dashboard uses a
	// default port.
	DashboardPort string

	// OutputDir is the directory where final output files will be written.
	// If nil, it defaults to `~/.shard`.
	OutputDir string

	// MapSplitSize is the size (in bytes) of each input split.
	// The Master will divide InputFiles into chunks of this size.
	// Each chunk becomes one Map task (M).
	//
	// If 0, Shard defaults to 64MB (64 * 1024 * 1024)
	MapSplitSize int64

	// InputFiles is the file which are to be processed
	InputFiles []string

	// Underlying abstraction for reading/writing
	Storer api.Storer
}

type Option func(*Config)

// WithMasterAddr sets the address of the Master (e.g., "localhost:5454").
func WithMasterAddr(addr string) Option {
	return func(c *Config) {
		c.MasterAddr = addr
	}
}

// WithRole sets the role to either Master or Worker.
func WithRole(role Role) Option {
	return func(c *Config) {
		c.Role = role
	}
}

// WithMapper sets the user's Map function.
func WithMapper(fn api.MapFunc) Option {
	return func(c *Config) {
		c.Mapper = fn
	}
}

// WithReducer sets the user's Reduce function.
func WithReducer(fn api.ReduceFunc) Option {
	return func(c *Config) {
		c.Reducer = fn
	}
}

// WithCombiner sets the optional Combiner function.
func WithCombiner(fn api.ReduceFunc) Option {
	return func(c *Config) {
		c.Combiner = fn
	}
}

// WithPartitioner sets the optional Partition function.
func WithPartitioner(fn api.PartitionFunc) Option {
	return func(c *Config) {
		c.Partitioner = fn
	}
}

// WithReduceTasks sets the number of output partitions (R).
func WithReduceTasks(n int) Option {
	return func(c *Config) {
		c.ReduceTasks = n
	}
}

// WithMapSplitSize sets the target size for Map tasks in bytes.
func WithMapSplitSize(size int64) Option {
	return func(c *Config) {
		c.MapSplitSize = size
	}
}

// WithDashboardPort sets the HTTP port for the dashboard.
func WithDashboardPort(port string) Option {
	return func(c *Config) {
		c.DashboardPort = port
	}
}

// WithOutputDir sets the directory for output files.
func WithOutputDir(dir string) Option {
	return func(c *Config) {
		c.OutputDir = dir
	}
}

// WithInputGlob accepts a glob pattern (e.g., "data/*.txt"), expands it into a
// list of files, and configures the job to use them.
func WithInputGlob(pattern string) Option {
	return func(c *Config) {
		if pattern == "" {
			return
		}

		files, err := filepath.Glob(pattern)
		if err != nil {
			slog.Error("failed to process input glob", "pattern", pattern, "error", err)
			os.Exit(1)
		}

		if len(files) == 0 {
			slog.Warn("input pattern matched no files", "pattern", pattern)
		}

		c.InputFiles = files
	}
}

func WithStorer(storer api.Storer) Option {
	return func(c *Config) {
		c.Storer = storer
	}
}

func defaultConfig() *Config {
	return &Config{
		ReduceTasks:   1,
		MapSplitSize:  64 * 1024 * 1026,
		DashboardPort: defaultPort,
		OutputDir:     defaultOutputDir,
	}
}

func NewConfig(opts ...Option) *Config {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	return cfg
}

func (cfg *Config) validate() error {
	if cfg.Mapper == nil {
		return errors.New("mapper function is required")
	}

	if cfg.Reducer == nil {
		return errors.New("reducer function is required")
	}

	if cfg.MasterAddr == "" {
		return errors.New("master addr cannot be empty")
	}

	if cfg.ReduceTasks <= 0 {
		return errors.New("reduce tasks must be greater than 0")
	}

	if cfg.Role != RoleMaster && cfg.Role != RoleWorker {
		return fmt.Errorf("invalid role '%s' (must be 'master' or 'worker')", cfg.Role)
	}

	if cfg.MapSplitSize <= 0 {
		return errors.New("map split size must be greater than 0")
	}

	return nil
}
