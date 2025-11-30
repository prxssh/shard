# Shard

A distributed MapReduce framework in Go, inspired by Google's MapReduce paper.

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Overview

Shard is a lightweight MapReduce implementation that lets you write distributed data processing applications with minimal boilerplate. Define your `Map` and `Reduce` functions, and Shard handles the rest—task scheduling, data partitioning, worker coordination, and fault tolerance.

> This is an educational implementation for learning distributed systems concepts. For production workloads, consider mature frameworks like Apache Hadoop or Apache Spark.

**Key Features:**
- Simple API - Write only Map and Reduce functions
- Automatic data partitioning across M map tasks and R reduce partitions
- Docker-ready with Docker Compose support
- Use as a library in your Go applications
- Based on Google's MapReduce paper

## Installation

```bash
go get github.com/prxssh/shard
```

## Quick Start

### 1. Import the Library

```go
import (
    "github.com/prxssh/shard"
    "github.com/prxssh/shard/api"
)
```

### 2. Define Map and Reduce Functions

```go
// Map: Split text into words and emit (word, "1")
func Map(key, document string) []api.KeyValue {
    words := strings.Fields(document)
    var results []api.KeyValue
    for _, word := range words {
        results = append(results, api.KeyValue{
            Key:   strings.ToLower(word),
            Value: "1",
        })
    }
    return results
}

// Reduce: Sum up counts for each word
func Reduce(key string, values []string) string {
    count := 0
    for _, v := range values {
        n, _ := strconv.Atoi(v)
        count += n
    }
    return strconv.Itoa(count)
}
```

### 3. Configure and Run

```go
func main() {
    logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
        Level: slog.LevelDebug,
    }))

    cfg := shard.Config{
        Mode:       shard.ModeMaster,
        Port:       ":1234",
        MasterAddr: "localhost:1234",
        NReduce:    8,
        InputFiles: []string{"data/input.txt"},
        Logger:     logger,
    }

    if err := shard.Run(cfg, Map, Reduce); err != nil {
        log.Fatal(err)
    }
}
```

### 4. Run Master and Workers

```bash
# Terminal 1 - Master
go run main.go -mode master -port :1234 -reduce 8 data/input.txt

# Terminal 2 - Worker
go run main.go -mode worker -port :8001 -master localhost:1234

# Terminal 3 - Worker
go run main.go -mode worker -port :8002 -master localhost:1234
```

## Using Docker Compose

See the included [examples/wordcount](examples/wordcount) for a complete working example:

```bash
docker-compose up --build
```

This runs:
- 1 master node processing Shakespeare's complete works
- 3 worker nodes executing map and reduce tasks

## Architecture

```
                    ┌──────────────┐
                    │    Master    │
                    │  Schedules   │
                    │    Tasks     │
                    └──────┬───────┘
                           │
            ┌──────────────┼──────────────┐
            │              │              │
       ┌────▼───┐     ┌────▼───┐    ┌────▼───┐
       │Worker 1│     │Worker 2│    │Worker N│
       │  Map   │     │  Map   │    │  Map   │
       │ Reduce │     │ Reduce │    │ Reduce │
       └────────┘     └────────┘    └────────┘
```

**How it works:**
1. Master splits input files into M chunks
2. Workers execute Map on chunks → intermediate key-value pairs
3. Intermediate data partitioned into R regions: `hash(key) % R`
4. Workers execute Reduce on partitions → final output

## API Reference

### Types

```go
// KeyValue is a key-value pair emitted by Map or Reduce
type KeyValue struct {
    Key   string
    Value string
}

// MapFunc processes input and emits intermediate pairs
type MapFunc func(key, value string) []KeyValue

// ReduceFunc aggregates values for each key
type ReduceFunc func(key string, values []string) string
```

### Config

```go
type Config struct {
    Mode       Mode         // shard.ModeMaster or shard.ModeWorker
    Port       string       // Port to bind (":1234")
    MasterAddr string       // Master address for workers
    InputFiles []string     // Input files (master only)
    NReduce    int          // Number of reduce partitions
    Logger     *slog.Logger // Optional logger

    // Optional: Override advertised address (for Docker/K8s)
    AdvertisedAddr string
}
```

### Constants

```go
const (
    ModeMaster Mode = "master"
    ModeWorker Mode = "worker"
)
```

### Main Function

```go
// Run starts the Shard engine
func Run(cfg Config, mapper api.MapFunc, reducer api.ReduceFunc) error
```

## Examples

### Word Count
Classic MapReduce example - count word frequency in text.

**Location:** [examples/wordcount](examples/wordcount)

**Dataset:** Shakespeare's complete works (~5.4MB)

**Try it:**
```bash
cd examples/wordcount
docker-compose -f ../../docker-compose.yml up --build
```

## Command Line Flags

| Flag | Description | Default |
|------|-------------|---------|
| `-mode` | "master" or "worker" | `master` |
| `-port` | Listen port | `:1234` |
| `-master` | Master address (workers) | `localhost:1234` |
| `-reduce` | Number of reduce partitions (R) | `8` |

Example:
```bash
go run main.go -mode worker -port :8080 -master master:1234
```

## Performance Tuning

### M (Map Tasks)
- Controlled by chunk size (default: 64MB)
- More tasks = better load balancing
- **Rule of thumb:** M ≈ 10-100x number of workers

### R (Reduce Partitions)
- Set with `-reduce` flag
- More partitions = better parallelism
- **Rule of thumb:** R ≈ 1-2x number of workers

## Contributing

Contributions are welcome! Whether it's:
- Bug fixes
- New examples
- Performance improvements
- Documentation updates
- Feature implementations

Please feel free to open issues or submit pull requests.

## Resources

- [MapReduce: Simplified Data Processing on Large Clusters](https://research.google/pubs/pub62/) - Google's original paper
- [MIT 6.824: Distributed Systems](https://pdos.csail.mit.edu/6.824/) - Distributed systems course

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Built with inspiration from:
- Google's MapReduce paper (Dean & Ghemawat, 2004)
- MIT 6.824 Distributed Systems course
