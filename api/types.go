package api

type KeyValue struct {
	Key   string
	Value string
}

type (
	MapFunc    func(key, val string) []KeyValue
	ReduceFunc func(key string, val []KeyValue) []KeyValue
)
