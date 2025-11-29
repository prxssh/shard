package main

import (
	"flag"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/prxssh/shard"
)

// --- 1. The Mapper ---
// Goal: Split the chunk into words and emit (word, "1")
func Map(document string) []api.KeyValue {
	// Split by anything that is NOT a letter or number (removes punctuation)
	splitter := func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}

	words := strings.FieldsFunc(document, splitter)

	var kv []api.KeyValue
	for _, w := range words {
		// Normalize to lowercase so "The" and "the" count as the same word
		w = strings.ToLower(w)
		if len(w) > 0 {
			kv = append(kv, api.KeyValue{
				Key:   w,
				Value: "1",
			})
		}
	}
	return kv
}

// --- 2. The Reducer ---
// Goal: Sum up the list of "1"s for a specific word
func Reduce(key string, values []string) string {
	count := 0
	for _, v := range values {
		i, _ := strconv.Atoi(v)
		count += i
	}
	return strconv.Itoa(count)
}

func main() {
	mode := flag.String("mode", "master", "Run mode: 'master' or 'worker'")
	port := flag.String("port", ":1234", "Port to listen on")
	masterAddr := flag.String("master", "localhost:1234", "Address of the master (for workers)")
	nReduce := flag.Int("reduce", 8, "Number of reduce buckets")

	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := shard.Config{
		Mode:       shard.Mode(*mode),
		Port:       *port,
		MasterAddr: *masterAddr,
		NReduce:    *nReduce,
		InputFiles: flag.Args(), // Any extra arguments are treated as input files
		Logger:     logger,
	}

	if err := shard.Run(cfg, Map, Reduce); err != nil {
		logger.Error("Shard engine failed", "error", err)
		os.Exit(1)
	}
}
