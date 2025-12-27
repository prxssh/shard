package main

import (
	"flag"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/prxssh/shard"
	"github.com/prxssh/shard/api"
)

func Map(key, value string) []api.KeyValue {
	var kv []api.KeyValue
	words := strings.FieldsFunc(key, func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	})

	for _, word := range words {
		word = strings.ToLower(word)
		if len(word) > 0 {
			kv = append(kv, api.KeyValue{Key: word, Value: "1"})
		}
	}

	return kv
}

func Reduce(key string, values []string) string {
	count := 0

	for _, value := range values {
		i, _ := strconv.Atoi(value)
		count += i
	}

	return strconv.Itoa(count)
}

func main() {
	mode := flag.String("mode", "master", "Run mode: 'master' or 'worker'")
	addr := flag.String("addr", ":6969", "Master address")
	inputGlob := flag.String("input", "", "Input files glob pattern (e.g., 'data/*.txt')")
	flag.Parse()

	cfg := shard.NewConfig(
		shard.WithRole(shard.Role(*mode)),
		shard.WithMasterAddr(*addr),
		shard.WithMapper(Map),
		shard.WithReducer(Reduce),
		shard.WithReduceTasks(10),
		shard.WithInputGlob(*inputGlob),
	)
	if err := shard.Run(cfg); err != nil {
		slog.Error("job failed", "error", err)
		os.Exit(1)
	}
}
