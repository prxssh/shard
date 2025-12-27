package master

import (
	"github.com/prxssh/shard/internal/rpc"
	"github.com/prxssh/shard/internal/task"
)

func (m *Master) AskTask(args *rpc.AskTaskArgs, reply *rpc.AskTaskReply) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	if t, allDone := m.scanTasks(m.mapTasks, args.WorkerID); !allDone {
		if t != nil {
			reply.Type = task.TypeMap
			reply.Task = t
		} else {
			reply.Type = task.TypeWait
		}

		return nil
	}

	if t, allDone := m.scanTasks(m.reduceTasks, args.WorkerID); !allDone {
		if t != nil {
			reply.Type = task.TypeReduce
			reply.Task = t
		} else {
			reply.Type = task.TypeWait
		}
	}

	reply.Type = task.TypeExit
	return nil
}
