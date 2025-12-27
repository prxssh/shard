package master

import "log/slog"

type Master struct {
	logger *slog.Logger
	cfg    *Config
}

func Start(inputFiles []string, cfg *Config, logger *slog.Logger) error {
	return nil
}
