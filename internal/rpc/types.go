package rpc

import (
	"github.com/google/uuid"
	"github.com/prxssh/shard/internal/task"
)

type AskTaskArgs struct {
	WorkerID uuid.UUID
}

type AskTaskReply struct {
	Type task.Type
	Task *task.Task
}

type ReportTaskArgs struct {
	WorkerID uuid.UUID
	TaskID   int64
	Type     task.Type
	Success  bool
}

type ReportTaskReply struct{}

type HearbeatArgs struct {
	WorkerID uuid.UUID
}

type HearbeatReply struct{}
