package worker

import (
	"bufio"
	"context"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prxssh/shard/api"
	pb "github.com/prxssh/shard/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const requestTimeout = 4 * time.Second

type Config struct {
	OutputDir      string
	InputPath      string
	NumReducers    int
	MaxConcurrency int
	MasterAddr     string
}

type Worker struct {
	id          uuid.UUID
	partitioner api.Partitioner
	mapper      api.Mapper
	reducer     api.Reducer
	fs          api.Filesystem
	logger      api.LoggerAdapter
	cfg         *Config

	workQueueCh chan *pb.TaskEntry

	client pb.CoordinatorClient
	conn   *grpc.ClientConn
}

func NewWorker(
	mapper api.Mapper,
	reducer api.Reducer,
	partitioner api.Partitioner,
	fs api.Filesystem,
	cfg *Config,
	logger api.LoggerAdapter,
) (*Worker, error) {
	workerID := uuid.New()

	conn, err := grpc.NewClient(
		cfg.MasterAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &Worker{
		id:          workerID,
		partitioner: partitioner,
		mapper:      mapper,
		reducer:     reducer,
		fs:          fs,
		logger:      logger.With(api.LogFields{"source": "worker", "id": workerID}),
		cfg:         cfg,
		conn:        conn,
		workQueueCh: make(chan *pb.TaskEntry, cfg.MaxConcurrency),
		client:      pb.NewCoordinatorClient(conn),
	}, nil
}

func (w *Worker) Start(ctx context.Context) error {
	defer w.conn.Close()

	var wg sync.WaitGroup

	wg.Go(func() { w.heartbeatLoop(ctx) })

	wg.Go(func() {
		defer close(w.workQueueCh)
		w.workFetcherLoop(ctx)
	})

	for i := 0; i < w.cfg.MaxConcurrency; i++ {
		wg.Go(func() { w.workerSlot(ctx, i) })
	}

	wg.Wait()

	return nil
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
			_, err := w.client.RecordHeartbeat(
				reqCtx,
				&pb.HeartbeatRequest{WorkerId: w.id.String()},
				nil,
			)
			cancel()
			if err != nil {
				w.logger.Error("heartbeat failed", err, nil)
			}
		}
	}
}

func (w *Worker) workFetcherLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			maxBuffer := cap(w.workQueueCh)
			availableSlots := maxBuffer - len(w.workQueueCh)
			if availableSlots < (maxBuffer >> 1) {
				continue
			}

			reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
			resp, err := w.client.PollTasks(
				reqCtx,
				&pb.TaskRequest{
					WorkerId:          w.id.String(),
					AvailableCapacity: uint32(availableSlots),
				},
				nil,
			)
			cancel()

			if err != nil {
				w.logger.Error("poll task failed", err, nil)
				continue
			}

			switch resp.GetAction() {
			case pb.TaskResponse_SHUTDOWN:
				w.logger.Info("received shutdown signal from master", nil)
				return

			case pb.TaskResponse_WAIT:
				continue

			case pb.TaskResponse_EXECUTE:
				for _, task := range resp.GetTasks() {
					select {
					case w.workQueueCh <- task:

					case <-ctx.Done():
						return
					}
				}

			default:
				w.logger.Info("unspecified action received from master", nil)
				continue
			}
		}
	}
}

func (w *Worker) workerSlot(ctx context.Context, idx int) {
	for task := range w.workQueueCh {
		w.logger.Info("processing task", api.LogFields{"id": task.GetTaskId(), "slot": idx})

		var err error

		switch payload := task.GetPayload().(type) {
		case *pb.TaskEntry_MapTask:
			err = w.performMapTask(ctx, payload.MapTask)
		case *pb.TaskEntry_ReduceTask:
			err = w.performReduceTask(ctx, payload.ReduceTask)
		}

		// FIXME (@prxssh): classify error as retryable or fatal
		if err != nil {
			w.logger.Error(
				"failed to perform task",
				err,
				api.LogFields{"id": task.GetTaskId(), "slot": idx},
			)

			w.reportTaskFailure(ctx, task.GetTaskId(), err.Error())
		}
	}
}

func (w *Worker) performMapTask(ctx context.Context, task *pb.MapTask) error {
	buf := newPartitionBuffer(w.partitioner, w.cfg.NumReducers)

	emitter := func(key, value string) error {
		buf.insert(key, value)
		return nil
	}

	f, err := w.fs.Open(task.GetInputFile())
	if err != nil {
		return err
	}
	defer f.Close()

	chunkSize := int64(task.GetLength())
	offset := int64(task.GetStartOffset())
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	scanner := bufio.NewScanner(f)
	if offset > 0 {
		if !scanner.Scan() {
			return scanner.Err()
		}
	}

	var bytesRead int64

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !scanner.Scan() {
			break
		}

		line := scanner.Text()
		if err := w.mapper(task.GetInputFile(), line, emitter); err != nil {
			return err
		}

		// Add +1 for the newline character stripped by the scanner.
		bytesRead += int64(len(line)) + 1
		if bytesRead >= chunkSize {
			break
		}

	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return buf.flush()
}

func (w *Worker) performReduceTask(ctx context.Context, task *pb.ReduceTask) error {
	return nil
}

func (w *Worker) reportTaskFailure(ctx context.Context, taskID uint64, message string) {
	reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	_, err := w.client.SubmitTaskResult(
		reqCtx,
		&pb.TaskResult{
			WorkerId:     w.id.String(),
			TaskId:       taskID,
			ErrorMessage: message,
			Status:       pb.TaskResult_FAILED_RETRYABLE,
		},
		nil,
	)
	cancel()

	if err != nil {
		w.logger.Error(
			"failed to report task failure to master",
			err,
			api.LogFields{"worker_id": w.id},
		)
	}
}
