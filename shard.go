package shard

func Run(cfg *Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}

	return nil
}
