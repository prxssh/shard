package coretype

type (
	MapFunc    func(key, value string) []KeyValue
	ReduceFunc func(key string, value []KeyValue) []KeyValue
)

func DefaultMapper(key, value string) []KeyValue {
	return nil
}

func DefaultReducer(key string, value []KeyValue) []KeyValue {
	return value
}

type KeyValue struct {
	Key   string
	Value string
}

type TaskType uint8

const (
	TaskMap TaskType = iota
	TaskReduce
)

func (tt TaskType) String() string {
	if tt == TaskMap {
		return "map"
	}
	return "reduce"
}

type Status uint8

const (
	StatusIdle Status = iota
	StatusProgress
	StatusCompleted
)

func (s Status) String() string {
	switch s {
	case StatusIdle:
		return "idle"
	case StatusProgress:
		return "progress"
	default:
		return "completed"
	}
}
