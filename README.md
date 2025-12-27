# Shard

> **A fault-tolerant, distributed MapReduce library in Go.**

[![Go Report Card](https://goreportcard.com/badge/github.com/prxssh/shard)](https://goreportcard.com/report/github.com/prxssh/shard)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


**Shard** is a clean, modern implementation of the MapReduce programming model
described in the seminal [OSDI 2004 paper](https://research.google.com/pubs/pub62/)
by Dean and Ghemawat. It provides a simple Go API for writing distributed data
processing jobs that automatically handles parallelization, fault tolerance,
and load balancing.

Designed as a library, `Shard` allows you to compile your MapReduce logic into
a single binary that can act as both a **Master** and a **Worker** based on
runtime configuration.

---

## Features

* **Pure Go Library:** No plugins or complex build steps. Import `shard` and
  run.
* **Fault Tolerance:** Automatically detects worker failures and re-schedules
  tasks. Handles "stragglers" via backup tasks.
* **Distributed Architecture:** Master-Worker design communicating via RPC.
* **Hybrid Configuration:** Use Go structs for logic and Environment
  Variables/Flags for infrastructure context (Docker/K8s friendly).
* **Combiner Functions:** Support for user-defined combiners to aggregate data
  locally on Map workers, significantly reducing network bandwidth usage.
* **Strict Sorting:** Guarantees that intermediate keys are processed in
  increasing order, enabling efficient random access lookups in output files.
* **Smart Input Splitting:** Automatically splits large input files into
  optimal chunks (16MB-64MB) for parallel processing.
* **Custom Reader Interface:** Pluggable input mechanism allowing you to
  process data from any source (SQL databases, JSON streams, CSV), not just
  text files.
* **Atomic Commits:** Ensures output consistency even in the presence of
  crashes.
* **Observability:** Built-in dashboard for real-time job monitoring and custom
  counter tracking.

---

## Installation and Quick Start

1. Download the package

```bash
go get github.com/prxssh/shard
```

2. Define your Map & Reduce Logic

Create a standard Go file. You need to define two functions matching the
`shard` interface.

```go
package main

import (
    "strings"
    "strconv"
    "github.com/prxssh/shard"
)

// Map: Splits line into words
func Map(filename string, contents string) []shard.KeyValue {
    kva := []shard.KeyValue{}
    words := strings.Fields(contents)

    for _, word := range words {
        kva = append(kva, shard.KeyValue{Key: w, Value: "1"})

        // Example: Using a custom counter
        if strings.Contains(word, "error") {
            shard.IncrementCounter("error_words")
        }
    }

    return kva
}

// Reduce: Sums counts for a word
func Reduce(key string, values []string) string {
    count := 0

    for _, v := range values {
        i, _ := strconv.Atoi(v)
        count += i
    }

    return strconv.Itoa(count)
}
```

3. Create the Entry point

```go
func main() {
    role := os.Getenv("SHARD_ROLE") // "master" or "worker"
    addr := os.Getenv("SHARD_MASTER") // e.g. "localhost:6969"

    cfg := shard.Config{
        // Address where master is running
        MasterAddr:  addr,
        // Role of the current instance (master or worker)
        Role:        role,
        // Map function
        Mapper:      Map,
        // Reduce function
        Reducer:     Reduce,
        // Combiner function (optional)
        Combiner:    Reduce,
        // Number of files that the reduce will output
        ReduceTasks: 10,
        // Dashboard port
        WebPort:     ":8080",
        // Location to save final output files. If not provided default value
        /(~/.shard) is used.
        OutputDir:   "./results"
    }

    shard.Run(cfg)
}
```

4. Run it

Compile your binary once:

```bash
go build -o wordcount
```

**Run the Master**:

```bash
export SHARD_ROLE=master
./wordcount inputs/*.txt
```

**Run the Workers (in separate terminals or containers)**:

```bash
export SHARD_ROLE=worker
export SHARD_MASTER=localhost:5454
./wordcount
```

## Architecture

Shard follows the classic Master-Worker architecture defined in the OSDI'04
paper:

1. **The Master (Coordinator)**:
    - Slices input files into Map tasks.
    - Maintains the state of all tasks.
    - Tracks worker health via heartbeats.
    - Acts as a conduit for propagating intermediate file locations from Map to
      Reduce.

2. **The Worker**:
    - Stateless execution unit.
    - Asks Master for work via RPC (`AskTask`).
    - Executes user's `Map` logic and partitions output into local intermediate
      files.
    - Executes user's `Reduce` logic by pulling data from Map workers (Shuffle).

## Observeability

The Master exposes a status page (default `:6969/status`) that displays:

- **Progress**: Completion precentage of Map and Reduce phases.
- **Counters**: Global aggregation of all user-defined counters.
- **Workers**: List of active workers and their last heartbeat time.
- **Throughput**: Estimated data processing rate.


## References

- [MapReduce: Simplified Data Processing on Large Clusters](https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/)
