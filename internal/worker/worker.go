package worker

import (
	"context"
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
	fileSystem  api.Storer
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
	fileSystem api.Storer,
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
		fileSystem:  fileSystem,
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
		w.logger.Info("processing task", api.LogFields{"id": task.TaskId, "slot": idx})
	}
}
