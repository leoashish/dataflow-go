package checkpoint

import "dataflow/internal/todo"

// Store keeps small named blobs (checkpoints) in a directory so they survive a crash.
//
// THE PROBLEM: os.WriteFile truncates the file and then writes. If the process dies in between -- or a reader looks in
// between -- the checkpoint is empty or half-written. A corrupted checkpoint is worse than none.
//
// THE FIX, used by every database and every well-behaved tool: write the new content to a TEMPORARY file in the same
// directory, flush it to disk (File.Sync), then os.Rename it over the real name. Rename within one filesystem is ATOMIC:
// a reader sees the complete old file or the complete new file, never a mixture.
//
//	Save(name, data): validate the name; create a unique temp file in the store's directory (os.CreateTemp with the
//	                  pattern name+".*.tmp"); write; Sync; Close; os.Rename(temp, dir/name). On any error remove the temp
//	                  file and return the error. (Extra credit: also fsync the directory so the rename itself is durable.)
//	Load(name):       the content, or an error wrapping ErrNotFound if it was never saved. Leftover *.tmp files from a
//	                  crashed Save must be ignored -- they have different names, so reading dir/name does this for free.
//	Names containing a path separator ("/" or "\"), or empty, "." or ".." are rejected with a real error (never write
//	outside the directory).
type Store struct{}

// NewStore creates the directory if needed (os.MkdirAll).
func NewStore(dir string) (*Store, error) {
	return nil, todo.NotImplemented("Ch07: NewStore")
}

func (s *Store) Save(name string, data []byte) error {
	return todo.NotImplemented("Ch07: Store.Save")
}

func (s *Store) Load(name string) ([]byte, error) {
	return nil, todo.NotImplemented("Ch07: Store.Load")
}
