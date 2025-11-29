package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/prxssh/shard/pkg/coretype"
	"github.com/prxssh/shard/pkg/logger"
	"github.com/prxssh/shard/pkg/master"
	"github.com/prxssh/shard/pkg/worker"
)

func main() {
	mode := flag.String("mode", "master", "Run mode: 'master' or 'worker'")
	port := flag.String("port", "1234", "Port to listen on")
	masterAddr := flag.String("master", "localhost:1234", "Address of the master node")
	flag.Parse()

	setupLogger()

	switch *mode {
	case "master":
		addr := ":" + *port
		m, err := master.NewMaster(addr, nil)
		if err != nil {
			slog.Error("failed to create master", "error", err.Error())
			os.Exit(1)
		}

		if err := m.Start(); err != nil {
			slog.Error("failed to start master", "error", err.Error())
			os.Exit(1)
		}
	case "worker":
		hostname, err := os.Hostname()
		if err != nil {
			slog.Error("failed to get hostname", "error", err.Error())
			os.Exit(1)
		}

		srcAddr := fmt.Sprintf("%s:%s", hostname, *port)

		w, err := worker.NewWorker(
			coretype.DefaultMapper,
			coretype.DefaultReducer,
			worker.WithMasterAddr(*masterAddr),
			worker.WithSrcAddr(srcAddr),
		)
		if err != nil {
			slog.Error("failed to create worker", "error", err.Error())
			os.Exit(1)
		}

		if err := w.Start(); err != nil {
			slog.Error("failed to start worker", "error", err.Error())
			os.Exit(1)
		}

	default:
		slog.Error("invalid mode, expected 'master' or 'worker'")
		os.Exit(1)
	}
}

func setupLogger() {
	opts := logger.DefaultOptions()
	opts.SlogOpts.Level = slog.LevelDebug
	opts.SlogOpts.AddSource = true

	h := logger.NewPrettyHandler(os.Stdout, &opts)
	l := slog.New(h)
	slog.SetDefault(l)
}
