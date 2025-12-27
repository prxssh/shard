package shard

import (
	"github.com/prxssh/shard/internal/master"
	"github.com/prxssh/shard/internal/worker"
	"github.com/prxssh/shard/pkg/fs"
)

func Run(cfg *Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}

	storer := cfg.Storer
	if storer == nil {
		storer = fs.NewLocalStorage()
	}

	if cfg.Role == RoleWorker {
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
			nil,
		)
	}

	return master.Start(
		cfg.InputFiles,
		&master.Config{
			SplitSize: cfg.MapSplitSize,
			NReduce:   cfg.ReduceTasks,
			Addr:      cfg.MasterAddr,
			OutputDir: cfg.OutputDir,
		},
		nil,
	)
}
