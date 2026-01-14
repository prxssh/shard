package worker

import (
	"context"

	"github.com/google/uuid"
	"github.com/prxssh/shard/api"
)

type Config struct {
	OutputDir      string
	InputPath      string
	NumReducers    int
	ChunkSize      int64
	MaxConcurrency int
}

type Worker struct {
	id          uuid.UUID
	partitioner api.Partitioner
	mapper      api.Mapper
	reducer     api.Reducer
	fileSystem  api.Storer
	logger      api.LoggerAdapter
	cfg         *Config
}

func NewWorker(
	mapper api.Mapper,
	reducer api.Reducer,
	partitioner api.Partitioner,
	fileSystem api.Storer,
	cfg *Config,
	logger api.LoggerAdapter,
) (*Worker, error) {
	return nil, nil
}

func (w *Worker) Start(ctx context.Context) error {
	return nil
}
