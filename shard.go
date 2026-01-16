package shard

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/prxssh/shard/api"
	"github.com/prxssh/shard/internal/master"
	"github.com/prxssh/shard/internal/worker"
)

func Run(cfg *Config) error {
	if envAddr := os.Getenv("SHARD_MASTER_ADDR"); envAddr != "" {
		cfg.MasterAddress = envAddr
	}
	if cfg.MasterAddress == "" {
		return fmt.Errorf("master address is required (set via config SHARD_MASTER_ADDR)")
	}

	mode := strings.ToLower(os.Getenv("SHARD_MODE"))
	if mode == "" {
		cfg.Logger.Info("SHARD_MODE not set, defaulting to 'master'", nil)
		mode = "master"
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cfg.Logger.Info("shutting down", nil)

		cancel()
	}()

	cfg.Logger.Info("starting", api.LogFields{
		"mode":        mode,
		"master_addr": cfg.MasterAddress,
		"output_dir":  cfg.OutputDir,
	})

	switch mode {
	case "master":
		master, err := master.NewMaster(
			&master.Config{
				Address:        cfg.MasterAddress,
				WorkerMaxTasks: cfg.MaxConcurrency,
				SplitSize:      cfg.ChunkSize,
				InputFiles:     cfg.inputFiles,
			},
			cfg.Logger,
		)
		if err != nil {
			return err
		}

		return master.Start(ctx)

	case "worker":
		worker, err := worker.NewWorker(
			cfg.Mapper,
			cfg.Reducer,
			cfg.Partitioner,
			cfg.Storer,
			&worker.Config{
				OutputDir:      cfg.OutputDir,
				InputPath:      cfg.InputPath,
				NumReducers:    cfg.NumReducers,
				MaxConcurrency: cfg.MaxConcurrency,
				ChunkSize:      cfg.ChunkSize,
				MasterAddr:     cfg.MasterAddress,
			},
			cfg.Logger,
		)
		if err != nil {
			return err
		}

		return worker.Start(ctx)

	default:
		return fmt.Errorf("unkown SHARD_MODE: %s", mode)
	}
}
