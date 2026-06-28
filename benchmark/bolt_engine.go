package benchmark

import (
	bolt "go.etcd.io/bbolt"
)

// boltBucketName is the single bucket every benchmark run uses inside
// BoltDB's file — irrelevant to the comparison itself, just a container
// BoltDB's API requires.
var boltBucketName = []byte("bench")

// boltEngine adapts BoltDB (a B+tree-based embedded KV store — the same
// general data structure SQLite uses internally, but exposed as a raw
// key-value API rather than through SQL) to the common engine interface.
//
// BoltDB is a genuinely useful comparison point distinct from SQLite:
// it's also a B-tree, but with none of SQL parsing/planning overhead in
// the way, so a BoltDB-vs-lsmdb comparison isolates "B-tree vs LSM-tree"
// more directly than a SQLite comparison does.
type boltEngine struct {
	db *bolt.DB
}

func newBoltEngine(path string) (*boltEngine, error) {
	db, err := bolt.Open(path, 0644, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(boltBucketName)
		return err
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &boltEngine{db: db}, nil
}

func (e *boltEngine) Put(key, value []byte) error {
	return e.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(boltBucketName).Put(key, value)
	})
}

func (e *boltEngine) Get(key []byte) ([]byte, bool, error) {
	var value []byte
	err := e.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(boltBucketName).Get(key)
		if v != nil {
			value = append([]byte{}, v...) // copy: v is only valid within the transaction
		}
		return nil
	})
	return value, value != nil, err
}

func (e *boltEngine) Scan(start, end []byte) ([][2][]byte, error) {
	var out [][2][]byte
	err := e.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(boltBucketName).Cursor()
		for k, v := c.Seek(start); k != nil; k, v = c.Next() {
			if end != nil && string(k) > string(end) {
				break
			}
			out = append(out, [2][]byte{
				append([]byte{}, k...),
				append([]byte{}, v...),
			})
		}
		return nil
	})
	return out, err
}

func (e *boltEngine) Close() error {
	return e.db.Close()
}
