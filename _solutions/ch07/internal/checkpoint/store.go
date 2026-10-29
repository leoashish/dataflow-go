package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Store struct {
	dir string
}

func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

func validName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("checkpoint: invalid name %q", name)
	}
	return nil
}

func (s *Store) Save(name string, data []byte) (err error) {
	if err := validName(name); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, name+".*.tmp") // same directory => same filesystem => rename is atomic
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()
	if _, err = tmp.Write(data); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil { // the bytes are on disk BEFORE the name points at them
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp.Name(), filepath.Join(s.dir, name)); err != nil {
		return err
	}
	if d, derr := os.Open(s.dir); derr == nil { // make the rename itself durable (not supported on every OS: best effort)
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

func (s *Store) Load(name string) ([]byte, error) {
	if err := validName(name); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filepath.Join(s.dir, name))
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	return data, err
}
