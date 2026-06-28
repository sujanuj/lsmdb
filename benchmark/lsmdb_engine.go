package benchmark

import (
	"github.com/sujanuj/lsmdb/internal/db"
)

// lsmdbEngine adapts db.DB to the common engine interface used by every
// benchmark in this package.
type lsmdbEngine struct {
	d *db.DB
}

func newLSMDBEngine(dir string) (*lsmdbEngine, error) {
	d, err := db.Open(dir)
	if err != nil {
		return nil, err
	}
	return &lsmdbEngine{d: d}, nil
}

func (e *lsmdbEngine) Put(key, value []byte) error {
	return e.d.Put(key, value)
}

func (e *lsmdbEngine) Get(key []byte) ([]byte, bool, error) {
	v, found := e.d.Get(key)
	return v, found, nil
}

func (e *lsmdbEngine) Scan(start, end []byte) ([][2][]byte, error) {
	it := e.d.Scan(start, end)
	var out [][2][]byte
	for {
		k, v, ok := it.Next()
		if !ok {
			break
		}
		out = append(out, [2][]byte{k, v})
	}
	return out, nil
}

func (e *lsmdbEngine) Close() error {
	return e.d.Close()
}
