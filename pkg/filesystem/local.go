package filesystem

import (
	"io"
	"os"
	"path/filepath"
)

type LocalFS struct{}

func NewLocalFS() *LocalFS {
	return &LocalFS{}
}

func (fs *LocalFS) Glob(pattern string) ([]string, error) {
	return filepath.Glob(pattern)
}

func (fs *LocalFS) Size(filename string) (int64, error) {
	info, err := os.Stat(filename)
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}

func (fs *LocalFS) Open(filename string) (io.ReadSeekCloser, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (fs *LocalFS) Create(filename string) (io.WriteCloser, error) {
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (fs *LocalFS) Delete(filename string) error {
	err := os.Remove(filename)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (fs *LocalFS) Abs(path string) (string, error) {
	return filepath.Abs(path)
}
