package task

import (
	"time"

	"github.com/google/uuid"
)

// Type represents the phase of the MapReduce job.
type Type uint8

const (
	// TypeMap indicates a task that processes a split of an input file.
	TypeMap Type = iota

	// TypeReduce indicates a task that aggregates intermediate data for a
	// specific partition.
	TypeReduce

	// TypeWait indicates the worker should sleep (used in RPC replies).
	TypeWait

	// TypeExit indicates the worker should shut down (used in RPC replies).
	TypeExit
)

// State tracks the lifecycle of a task within the Master's scheduler.
type State uint8

const (
	// StateIdle means the task is waiting in the queue and can be assigned to
	// a worker.
	StateIdle State = iota

	// StateProgress means a worker has picked up the task and is currently
	// processing it.
	StateProgress

	// StateCompleted means the task has finished successfully and output is
	// generated.
	StateCompleted
)

// Task represents a single unit of work to be distributed to a worker.
type Task struct {
	// ID is the globally unique identifier for this task.
	ID int64

	// File is the path to the input file being processed.
	File string

	// Offset is the byte offset where the split begins in the input file.
	Offset int64

	// Len is the length of the split in bytes.
	Len int64

	// State tracks if the task is Idle, In-Progress, or Completed.
	State State

	// Type determines if this is a Map or Reduce task.
	Type Type

	// ReducePartitions is the total number of reduce partitions.
	// Mappers need this to correctly hash keys into buckets (hash(key) % NReduce).
	ReducePartitions int

	// ReduceID is the partition number this task is responsible for (0 to NReduce-1).
	// The worker uses this to locate intermediate files.
	ReduceID int

	// StartTime, as the name itself suggests, is the time at which this task
	// was assigned to a worker.
	StartTime time.Time

	// WorkerID represents the unique identifier of the worker executing this
	// task.
	WorkerID uuid.UUID
}
