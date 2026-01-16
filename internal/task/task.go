package task

import (
	"time"
)

// Status represents the state of a Task in its lifetime.
type Status uint8

const (
	// StatusPending indicates the task is queued and waiting for an available
	// worker.
	StatusPending Status = iota

	// StatusExecuting indicates the task is currently being processed by a
	// worker.
	StatusExecuting

	// StatusCancelled indicates the task was manually stopped before
	// completion.
	StatusCancelled

	// StatusCompleted indicates the task finished successfully.
	StatusCompleted

	// StatusDiscarded indicates the task was removed due to error or
	// invalidity.
	StatusDiscarded
)

// Task represents a specific unit of work associated with a portion of a file.
type Task struct {
	// ID is the unique numeric identifier for the task instance.
	ID uint64

	// InputFile is the absolute path identifying the source data.
	InputFile string

	// Offset defines the byte position within the InputFile where processing
	// should begin.
	Offset int64

	// Status tracks the current execution state of the task.
	Status Status

	// StartedAt records the timestamp when the task execution began.
	StartedAt time.Time

	// CompletedAt records the timestamp when the task reached a terminal state
	// (Completed, Cancelled, or Discarded).
	CompletedAt time.Time
}
