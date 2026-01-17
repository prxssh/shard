package master

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prxssh/shard/api"
)

type Config struct {
	Address        string
	WorkerMaxTasks int
	SplitSize      int64
	InputFiles     []string
}

type Worker struct {
	assignedTasks   []uint
	lastHeartbeatAt time.Time
}

type Stats struct {
	Progressing atomic.Uint32
	Completed   atomic.Uint32
	Failures    atomic.Uint32
}

type Master struct {
	cfg     *Config
	logger  api.LoggerAdapter
	stats   *Stats
	workers map[uuid.UUID]*Worker
}

func NewMaster(cfg *Config, logger api.LoggerAdapter) (*Master, error) {
	return nil, nil
}

func (m *Master) Start(ctx context.Context) error {
	return nil
}
