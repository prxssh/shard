package master

import (
	"time"

	"github.com/google/uuid"
	"github.com/prxssh/shard/internal/rpc"
	"github.com/prxssh/shard/internal/task"
)

func (m *Master) AskTask(args *rpc.AskTaskArgs, reply *rpc.AskTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.logger.Info("received task request from worker", "worker-id", args.WorkerID)

	if t, phaseComplete := m.findAndAssignTask(m.mapTasks, args.WorkerID); !phaseComplete {
		if t != nil {
			m.logger.Info(
				"assigning map task to worker",
				"worker-id", args.WorkerID,
				"task-id", t.ID,
			)
			reply.Type = task.TypeMap
			reply.Task = t
		} else {
			reply.Type = task.TypeWait
		}

		return nil
	}

	if t, phaseComplete := m.findAndAssignTask(m.reduceTasks, args.WorkerID); !phaseComplete {
		if t != nil {
			m.logger.Info(
				"assigning reduce task to worker",
				"worker-id", args.WorkerID,
				"task-id", t.ID,
			)
			reply.Type = task.TypeReduce
			reply.Task = t
		} else {
			reply.Type = task.TypeWait
		}
		return nil
	}

	reply.Type = task.TypeExit
	return nil
}

func (m *Master) WorkerHeartbeat(args *rpc.HearbeatArgs, reply *rpc.HearbeatReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	worker, ok := m.workers[args.WorkerID]
	if !ok {
		m.logger.Warn("received heartbeat from unknown worker", "worker-id", args.WorkerID)
		return nil
	}

	m.logger.Debug("received heartbeat from worker", "worker-id", args.WorkerID)
	worker.lastActivityAt = time.Now()
	return nil
}

func (m *Master) ReportTask(args *rpc.ReportTaskArgs, reply *rpc.ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.logger.Info(
		"received task report from worker",
		"worker-id", args.WorkerID,
		"task-id", args.TaskID,
		"success", args.Success,
	)

	var tasks []*task.Task
	switch args.Type {
	case task.TypeMap:
		tasks = m.mapTasks
	case task.TypeReduce:
		tasks = m.reduceTasks
	default:
		m.logger.Warn("unknown task type in report", "type", args.Type)
		return nil
	}

	var foundTask *task.Task
	for _, t := range tasks {
		if t.ID == args.TaskID {
			foundTask = t
			break
		}
	}

	if foundTask == nil {
		m.logger.Warn("reported task not found", "task-id", args.TaskID)
		return nil
	}

	if foundTask.WorkerID != args.WorkerID {
		m.logger.Warn(
			"ignoring report from stale worker",
			"stale-worker-id", args.WorkerID,
			"current-worker-id", foundTask.WorkerID,
			"task-id", args.TaskID,
		)
		return nil
	}

	if args.Success {
		foundTask.State = task.StateCompleted
		m.stats.completed.Add(1)
		m.stats.progressing.Add(^uint64(0))

		if w, ok := m.workers[args.WorkerID]; ok {
			w.completedCount.Add(1)
			w.currentTask = nil
		}

		return nil
	}

	m.logger.Warn("task failed resetting", "task-id", args.TaskID, "worker", args.WorkerID)

	foundTask.State = task.StateIdle
	foundTask.WorkerID = uuid.Nil
	foundTask.StartTime = time.Time{}

	m.stats.failed.Add(1)
	m.stats.progressing.Add(^uint64(0))

	if w, ok := m.workers[args.WorkerID]; ok {
		w.failedCount.Add(1)
		w.currentTask = nil
	}

	return nil
}
