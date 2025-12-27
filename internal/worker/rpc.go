package worker

import (
	"net/rpc"

	shardrpc "github.com/prxssh/shard/internal/rpc"
	"github.com/prxssh/shard/internal/task"
)

func (w *Worker) sendHeartbeat() error {
	args := shardrpc.HearbeatArgs{WorkerID: w.id}
	var reply shardrpc.HearbeatReply

	return w.invokeRPC("Master.WorkerHeartbeat", &args, &reply)
}

func (w *Worker) reportTask(taskID int64, taskType task.Type, taskErr error) error {
	args := shardrpc.ReportTaskArgs{
		WorkerID: w.id,
		TaskID:   taskID,
		Type:     taskType,
		Success:  taskErr == nil,
	}
	var reply shardrpc.ReportTaskReply

	return w.invokeRPC("Master.ReportTask", &args, &reply)
}

func (w *Worker) askTask() (shardrpc.AskTaskReply, error) {
	args := shardrpc.AskTaskArgs{WorkerID: w.id}
	var reply shardrpc.AskTaskReply

	if err := w.invokeRPC("Method.AskTask", &args, &reply); err != nil {
		return shardrpc.AskTaskReply{}, err
	}
	return reply, nil
}

func (w *Worker) invokeRPC(method string, args, reply any) error {
	w.logger.Debug("invoking rpc", "method", method)

	client, err := rpc.DialHTTP("tcp", w.cfg.MasterAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Call(method, args, reply)
}
