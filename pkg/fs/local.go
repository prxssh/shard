package fs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Local struct{}

func NewLocalStorage() *Local {
	return &Local{}
}

func (l *Local) OpenRead(path string, offset, length int64) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	if offset == 0 && length == -1 {
		return f, nil
	}

	if _, err := f.Seek(offset, 0); err != nil {
		f.Close()
		return nil, err
	}

	if length > 0 {
		return &limitReadCloser{
			r: io.LimitReader(f, length),
			c: f,
		}, nil
	}

	return f, nil
}

func (l *Local) OpenWrite(path string) (io.WriteCloser, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("fs: failed to create directory %s: %w", dir, err)
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return f, nil
}

type limitReadCloser struct {
	r io.Reader
	c io.Closer
}

func (l *limitReadCloser) Read(p []byte) (int, error) {
	return l.r.Read(p)
}

func (l *limitReadCloser) Close() error {
	return l.c.Close()
}
