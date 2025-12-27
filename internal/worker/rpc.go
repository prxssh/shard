package worker

import (
	"net/rpc"

	shardrpc "github.com/prxssh/shard/internal/rpc"
	"github.com/prxssh/shard/internal/task"
)

func (w *Worker) pingMaster() error {
	args := shardrpc.HearbeatArgs{WorkerID: w.id}
	var reply shardrpc.HearbeatReply

	return w.callMaster("Master.WorkerHeartbeat", &args, &reply)
}

func (w *Worker) report(taskID int64, taskType task.Type, taskErr error) error {
	args := shardrpc.ReportTaskArgs{
		WorkerID: w.id,
		TaskID:   taskID,
		Type:     taskType,
		Success:  taskErr == nil,
	}
	var reply shardrpc.ReportTaskReply

	return w.callMaster("Master.ReportTask", &args, &reply)
}

func (w *Worker) requestWork() (shardrpc.AskTaskReply, error) {
	args := shardrpc.AskTaskArgs{WorkerID: w.id}
	var reply shardrpc.AskTaskReply

	if err := w.callMaster("Method.AskTask", &args, &reply); err != nil {
		return shardrpc.AskTaskReply{}, err
	}
	return reply, nil
}

func (w *Worker) callMaster(method string, args, reply any) error {
	client, err := rpc.DialHTTP("tcp", w.cfg.MasterAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Call(method, args, reply)
}
