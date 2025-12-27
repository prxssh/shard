package shard

import (
	"log/slog"
	"os"

	"github.com/prxssh/shard/internal/master"
	"github.com/prxssh/shard/internal/worker"
	"github.com/prxssh/shard/pkg/fs"
)

func Run(cfg *Config) error {
	logger := slog.New(
		slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}),
	)

	if err := cfg.validate(); err != nil {
		logger.Error("Failed to validate config", "err", err)
		return err
	}

	storer := cfg.Storer
	if storer == nil {
		storer = fs.NewLocalStorage()
	}

	if cfg.Role == RoleWorker {
		logger.Info("Starting worker")
		return worker.Start(
			storer,
			&worker.Config{
				MasterAddr:  cfg.MasterAddr,
				Partitioner: cfg.Partitioner,
				Mapper:      cfg.Mapper,
				Reducer:     cfg.Reducer,
				Combiner:    cfg.Combiner,
				OutputDir:   cfg.OutputDir,
			},
			logger,
		)
	}

	logger.Info("Starting master")

	return master.Start(
		cfg.InputFiles,
		&master.Config{
			MapSplitSize:     cfg.MapSplitSize,
			ReducePartitions: cfg.ReduceTasks,
			ListenAddr:       cfg.MasterAddr,
			OutputDir:        cfg.OutputDir,
			WorkerTimeout:    cfg.WorkerInactivityDuration,
		},
		logger,
	)
}
