package worker

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prxssh/shard/api"
	pb "github.com/prxssh/shard/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	requestTimeout      = 4 * time.Second
	maxScannerTokenSize = 10 * 1024 * 1024

	IntermediateDir = "intermediate"
	FinalDir        = "final"
)

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
	combiner    api.Combiner
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
	combiner api.Combiner,
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
		combiner:    combiner,
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
		taskID := task.GetTaskId()
		w.logger.Info("processing task", api.LogFields{"id": taskID, "slot": idx})

		var err error

		switch payload := task.GetPayload().(type) {
		case *pb.TaskEntry_MapTask:
			err = w.performMapTask(ctx, taskID, payload.MapTask)
		case *pb.TaskEntry_ReduceTask:
			err = w.performReduceTask(ctx, taskID, payload.ReduceTask)
		}

		// FIXME (@prxssh): classify error as retryable or fatal
		if err != nil {
			w.logger.Error(
				"failed to perform task",
				err,
				api.LogFields{"id": taskID, "slot": idx},
			)

			w.reportTaskFailure(ctx, taskID, err.Error())
			continue
		}

		w.reportTaskSuccess(ctx, taskID)
	}
}

func (w *Worker) performMapTask(ctx context.Context, taskID uint64, task *pb.MapTask) error {
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
	bufFunc := make([]byte, maxScannerTokenSize)
	scanner.Buffer(bufFunc, maxScannerTokenSize)

	// Skip partial line if we aren't at the start
	if offset > 0 {
		if !scanner.Scan() {
			return scanner.Err()
		}
	}

	var bytesRead int64
	intermediatePath := filepath.Join(w.cfg.OutputDir, IntermediateDir)
	buf := newPartitionBuffer(
		taskID,
		w.partitioner,
		w.combiner,
		w.cfg.NumReducers,
		w.fs,
		intermediatePath,
	)
	emitter := func(key, value string) error {
		buf.insert(key, value)
		return nil
	}

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := scanner.Text()
		readLen := int64(len(scanner.Bytes())) + 1
		bytesRead += readLen

		if err := w.mapper(task.GetInputFile(), line, emitter); err != nil {
			return err
		}

		if bytesRead >= chunkSize {
			break
		}

	}
	if err := scanner.Err(); err != nil {
		return err
	}

	return buf.flush()
}

func (w *Worker) performReduceTask(ctx context.Context, taskID uint64, task *pb.ReduceTask) error {
	pattern := filepath.Join(
		w.cfg.OutputDir,
		IntermediateDir,
		fmt.Sprintf("partition-%d-task-*.shard", task.GetPartitionId()),
	)

	files, err := w.fs.Glob(pattern)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}

	mergeIter, err := newMergeIterator(files, w.fs)
	if err != nil {
		return err
	}
	defer mergeIter.Close()

	finalPath := filepath.Join(
		w.cfg.OutputDir,
		FinalDir,
		fmt.Sprintf("part-%d.shard", task.GetPartitionId()),
	)
	outFile, err := w.fs.Create(finalPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	emit := func(k, v string) error {
		_, err := fmt.Fprintf(outFile, "%s\t%s\n", k, v)
		return err
	}

	var (
		currKey   string
		valBuffer []string
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		k, v, ok := mergeIter.Next()
		if !ok {
			if len(valBuffer) > 0 {
				if err := w.reducer(currKey, &sliceIterator{values: valBuffer}, emit); err != nil {
					return err
				}
			}

			if err := mergeIter.Error(); err != nil {
				return err
			}
			break
		}

		if k != currKey && len(valBuffer) > 0 {
			if err := w.reducer(currKey, &sliceIterator{values: valBuffer}, emit); err != nil {
				return err
			}
			valBuffer = nil
		}

		currKey = k
		valBuffer = append(valBuffer, v)
	}

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

func (w *Worker) reportTaskSuccess(ctx context.Context, taskID uint64) {
	reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	_, err := w.client.SubmitTaskResult(
		reqCtx,
		&pb.TaskResult{
			WorkerId: w.id.String(),
			TaskId:   taskID,
			Status:   pb.TaskResult_SUCCESS,
		},
		nil,
	)
	cancel()

	if err != nil {
		w.logger.Error(
			"failed to report task success to master",
			err,
			api.LogFields{"worker_id": w.id},
		)
	}
}
