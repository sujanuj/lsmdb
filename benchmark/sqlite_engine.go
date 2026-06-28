package benchmark

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

// sqliteEngine adapts SQLite (via the database/sql standard interface)
// to the common engine interface. A single table with a TEXT/BLOB
// primary key and BLOB value column is the closest equivalent to a raw
// key-value store SQLite offers — this intentionally does NOT try to
// showcase SQL features, since the comparison is about storage engine
// characteristics (B-tree vs LSM-tree), not query language convenience.
//
// journal_mode=WAL is set explicitly because it's the realistic
// production configuration (better concurrent read/write behavior than
// SQLite's older rollback-journal default) — comparing lsmdb against
// SQLite's worst-case default settings would be a less honest
// comparison than comparing against how SQLite actually gets deployed.
type sqliteEngine struct {
	db *sql.DB
}

func newSQLiteEngine(path string) (*sqliteEngine, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, err
		}
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB)`); err != nil {
		db.Close()
		return nil, err
	}
	return &sqliteEngine{db: db}, nil
}

func (e *sqliteEngine) Put(key, value []byte) error {
	_, err := e.db.Exec(`INSERT INTO kv (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v`, key, value)
	return err
}

func (e *sqliteEngine) Get(key []byte) ([]byte, bool, error) {
	var value []byte
	err := e.db.QueryRow(`SELECT v FROM kv WHERE k = ?`, key).Scan(&value)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (e *sqliteEngine) Scan(start, end []byte) ([][2][]byte, error) {
	rows, err := e.db.Query(`SELECT k, v FROM kv WHERE k >= ? AND k <= ? ORDER BY k`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out [][2][]byte
	for rows.Next() {
		var k, v []byte
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out = append(out, [2][]byte{k, v})
	}
	return out, rows.Err()
}

func (e *sqliteEngine) Close() error {
	return e.db.Close()
}
