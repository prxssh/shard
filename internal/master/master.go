package master

import (
	"context"

	"github.com/prxssh/shard/api"
)

type Config struct {
	Address        string
	MaxConcurrency int
}

type Master struct {
	cfg    *Config
	logger api.LoggerAdapter
}

func NewMaster(cfg *Config, logger api.LoggerAdapter) (*Master, error) {
	return nil, nil
}

func (m *Master) Start(ctx context.Context) error {
	return nil
}
