# shard

`shard` is a lightweight, easy-to-use MapReduce framework for Go. It provides a
simple and flexible way to write and run distributed computations on a cluster
of machines.

## Features

*   **Simple API:** `shard` provides a simple and intuitive API for writing
    MapReduce programs.
*   **Pluggable Components:** `shard` allows you to bring your own `Mapper`,
    `Reducer`, `Combiner`, `Partitioner`, and `Filesystem` implementations.
*   **Master-Worker Architecture:** `shard` uses a master-worker architecture
    to distribute and manage tasks.
*   **gRPC for Communication:** `shard` uses gRPC for efficient and reliable
    communication between the master and worker nodes.

## Installation

To install `shard`, use `go get`:

```bash
go get github.com/prxssh/shard
```

## Configuration

`shard` can be configured using environment variables or through the `Config`
struct.

| Environment Variable | `Config` Field      | Description                               | Default                             |
| -------------------- | ------------------- | ----------------------------------------- | ----------------------------------- |
| `SHARD_MODE`         | -                   | The mode to run in (`master` or `worker`). | `master`                            |
| `SHARD_MASTER_ADDR`  | `MasterAddress`     | The address of the master node.           | `localhost:6969`                    |
| -                    | `InputPath`         | The path to the input file or directory.  | -                                   |
| -                    | `OutputDir`         | The path to the output directory.         | `./shard`                           |
| -                    | `NumReducers`       | The number of reduce tasks.               | `16`                                |
| -                    | `ChunkSize`         | The size of each input split.             | `64MB`                              |
| -                    | `MaxConcurrency`    | The maximum number of concurrent tasks.   | `runtime.NumCPU() * 2`              |

## Usage

> This project is written just for learning purposes and breaking changes are
> to be expected.

Here is an example of how to use `shard` to implement a word count program:

```go
package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/prxssh/shard"
	"github.com/prxssh/shard/api"
	"github.com/prxssh/shard/pkg/filesystem"
)

func main() {
	// Create a new shard config.
	cfg, err := shard.NewConfig(
		shard.WithInputPath("input.txt"),
		shard.WithMapper(Map),
		shard.WithReducer(Reduce),
		shard.WithFilesystem(filesystem.NewLocal()),
	)
	if err != nil {
		panic(err)
	}

	// Run the shard job.
	if err := shard.Run(cfg); err != nil {
		panic(err)
	}
}

// Map is a mapper that emits a count for each word.
func Map(key, value string, emit api.Emitter) error {
	words := strings.Fields(value)
	for _, word := range words {
		if err := emit(word, "1"); err != nil {
			return err
		}
	}
	return nil
}

// Reduce is a reducer that sums the counts for each word.
func Reduce(key string, values api.Iterator, emit api.Emitter) error {
	count := 0
	for {
		_, ok := values.Next()
		if !ok {
			break
		}
		count++
	}

	return emit(key, strconv.Itoa(count))
}
```

## Development

Information for developers, including how to run tests and generate protobuf files.

### Running Tests

To run the tests, use the following command:

```bash
make test
```

### Generating Protobuf Files

To generate the protobuf files, use the following command:

```bash
make gen-proto FILE=path/to/file.proto
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file
for details.
