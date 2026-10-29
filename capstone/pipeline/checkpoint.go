package pipeline

import (
	"dataflow/capstone/model"
	"dataflow/internal/todo"
)

// SaveCheckpoint durably stores the state and the input position it corresponds to, atomically: a crash at ANY moment must
// leave either the previous complete checkpoint or the new one, never a torn file.
//
// TODO(capstone): chapter 07 (atomic write: temp file + fsync + rename) and chapter 22 (idempotent application).
// Think about which sketches can be serialised (you may need to add MarshalBinary methods to your own sketch types).
func SaveCheckpoint(dir string, s *State, pos Position) error {
	return todo.NotImplemented("capstone: SaveCheckpoint")
}

// LoadCheckpoint restores the latest checkpoint from dir. No checkpoint yet is NOT an error: return a fresh state and the zero Position.
func LoadCheckpoint(dir string, cfg model.Config) (*State, Position, error) {
	return nil, Position{}, todo.NotImplemented("capstone: LoadCheckpoint")
}
