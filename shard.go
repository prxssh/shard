package shard

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/prxssh/shard/api"
	"github.com/prxssh/shard/internal/master"
	"github.com/prxssh/shard/internal/worker"
)

type Mode string

const (
	ModeMaster Mode = "master"
	ModeWorker Mode = "worker"
)

type Config struct {
	Mode       Mode
	Port       string   // ":1234"
	MasterAddr string   // "localhost:1234"
	InputFiles []string // Only for master
	NReduce    int      // Only for master
	Logger     *slog.Logger

	// AdvertisedAddr is optional.
	// If empty, the worker will automatically determine "hostname:port".
	// Set this if running behind NAT, Docker port mappings, or K8s NodePorts.
	AdvertisedAddr string
}

// Run starts the Shard engine based on the config.
// It blogs until completion or error
func Run(cfg Config, mapper api.MapFunc, reducer api.ReduceFunc) error {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	port, err := parsePort(cfg.Port)
	if err != nil {
		return fmt.Errorf("invalid port: %w", err)
	}

	switch cfg.Mode {
	case ModeMaster:
		m, err := master.NewMaster(
			cfg.InputFiles,
			cfg.NReduce,
			master.WithPort(port),
			master.WithLogger(cfg.Logger),
		)
		if err != nil {
			return fmt.Errorf("failed to initialize master: %w", err)
		}

		return m.Start()

	case ModeWorker:
		opts := []worker.Option{
			worker.WithMasterAddr(cfg.MasterAddr),
			worker.WithPort(port),
			worker.WithLogger(cfg.Logger),
		}
		if cfg.AdvertisedAddr != "" {
			opts = append(opts, worker.WithAdvertisedAddr(cfg.AdvertisedAddr))
		}

		w, err := worker.NewWorker(mapper, reducer, opts...)
		if err != nil {
			return fmt.Errorf("failed to initialize worker: %w", err)
		}

		return w.Start()

	default:
		return errors.New("invalid mode specified; allowed 'master' and 'worker'")
	}
}

func parsePort(p string) (int, error) {
	p = strings.TrimPrefix(p, ":")
	if p == "" {
		return 0, errors.New("port cannot be empty")
	}
	return strconv.Atoi(p)
}
