package raftsqlite3

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	metrics "github.com/armon/go-metrics"
	"github.com/hashicorp/raft"
	_ "github.com/mattn/go-sqlite3"
)

var (
	// An error indicating a given key does not exist
	ErrKeyNotFound = errors.New("not found")
)

type AccessMode int

const (
	AccessDefault = iota
	AccessRO
	AccessRW
	AccessRWC
	AccessMemory
)

func (a AccessMode) String() string {
	switch a {
	case AccessRO:
		return "ro"
	case AccessRW:
		return "rw"
	case AccessRWC:
		return "rwc"
	case AccessMemory:
		return "memory"
	default:
		return ""
	}
}

// Sqlite3Store provides access to Sqlite3 for Raft to store and retrieve
// log entries. It also provides key/value storage, and can be used as
// a LogStore and StableStore.
type Sqlite3Store struct {
	// db is the underlying handle to the db
	db *sql.DB

	// The path to the Bolt database file
	path string

	mode AccessMode

	pragmas []Pragmaer
}

type OptFunc func(*Sqlite3Store)

func WithMode(m AccessMode) OptFunc {
	return func(s *Sqlite3Store) {
		s.mode = m
	}
}

func WithPragmas(pragmas ...Pragmaer) OptFunc {
	return func(s *Sqlite3Store) {
		s.pragmas = pragmas
	}
}

// New uses the supplied options to open the Sqlite3 and prepare it for use as a raft backend.
func New(path string, opts ...OptFunc) (*Sqlite3Store, error) {
	store := &Sqlite3Store{path: path}
	for _, o := range opts {
		o(store)
	}

	dsn := ""
	if path != "" {
		dsn = "file:" + path

		if store.mode != AccessDefault {
			dsn += "?mode=" + store.mode.String()
		}
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	store.db = db

	// Set up our buckets
	if store.mode != AccessRO {
		if err := store.initialize(); err != nil {
			store.Close()
			return nil, err
		}
	}
	return store, nil
}

const logsTableSQL = `
CREATE TABLE IF NOT EXISTS logs (
  idx BLOB PRIMARY KEY,
  term BLOB,
  type INTEGER,
  data BLOB,
  extensions BLOB,
  appendedAt INTEGER
);
`

const confTableSQL = `
CREATE TABLE IF NOT EXISTS conf (
  name BLOB PRIMARY KEY,
  value BLOB
);
`

// initialize is used to set up all of the buckets.
func (b *Sqlite3Store) initialize() error {
	tx, err := b.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// go through all the pragmas from the options.
	for _, p := range b.pragmas {
		_, err := b.db.Exec(fmt.Sprintf("PRAGMA %s = %s", p.Pragma(), p.String()))
		if err != nil {
			return err
		}
	}

	// Create the Logs table
	_, err = tx.Exec(logsTableSQL)
	if err != nil {
		return err
	}

	_, err = tx.Exec(confTableSQL)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// Close is used to gracefully close the DB connection.
func (b *Sqlite3Store) Close() error {
	return b.db.Close()
}

// FirstIndex returns the first known index from the Raft log.
func (b *Sqlite3Store) FirstIndex() (uint64, error) {
	var idx []byte
	err := b.db.QueryRow(`SELECT min(idx) FROM logs`).Scan(&idx)
	switch {
	case err == sql.ErrNoRows:
		return 0, nil
	case err != nil:
		return 0, err
	default:
		if len(idx) == 0 {
			return 0, nil
		}
		return bytesToUint64(idx), nil
	}
}

// LastIndex returns the last known index from the Raft log.
func (b *Sqlite3Store) LastIndex() (uint64, error) {
	var idx []byte
	err := b.db.QueryRow(`SELECT max(idx) FROM logs`).Scan(&idx)
	switch {
	case err == sql.ErrNoRows:
		return 0, nil
	case err != nil:
		return 0, err
	default:
		if len(idx) == 0 {
			return 0, nil
		}
		return bytesToUint64(idx), nil
	}
}

// GetLog is used to retrieve a log from Sqlite3 at a given index.
func (b *Sqlite3Store) GetLog(idx uint64, log *raft.Log) error {
	now := time.Now()
	var (
		bidx, bterm []byte
		ts          int64
	)

	bidx = uint64ToBytes(idx)

	defer func() {
		metrics.MeasureSince([]string{"raft", "sqlite3", "getLog"}, now)
	}()

	err := b.db.QueryRow(`SELECT
idx, term, type, data, extensions, appendedAt
FROM logs WHERE idx = ?`, bidx).Scan(
		&bidx, &bterm, &(log.Type), &(log.Data),
		&(log.Extensions), &ts,
	)
	switch {
	case err == sql.ErrNoRows:
		return raft.ErrLogNotFound
	case err != nil:
		return err
	default:
		log.Index = idx
		log.Term = bytesToUint64(bterm)
		log.AppendedAt = nanosToTime(ts)
		return nil
	}
}

// StoreLog is used to store a single raft log
func (b *Sqlite3Store) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store a set of raft logs
func (b *Sqlite3Store) StoreLogs(logs []*raft.Log) error {
	now := time.Now()
	tx, err := b.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	batchSize := 0

	stmt, err := tx.Prepare(`
INSERT INTO logs (
  idx, term, type, data, extensions, appendedAt
) VALUES (?, ?, ?, ?, ?, ?)
`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, log := range logs {
		idx := uint64ToBytes(log.Index)
		term := uint64ToBytes(log.Term)
		ts := timeToNanos(log.AppendedAt)

		_, err := stmt.Exec(idx, term, log.Type, log.Data, log.Extensions, ts)
		if err != nil {
			return err
		}

		logLen := len(log.Data) + len(log.Extensions)

		batchSize += logLen
		metrics.AddSample([]string{"raft", "sqlite3", "logSize"}, float32(logLen))
	}

	metrics.AddSample([]string{"raft", "sqlite3", "logsPerBatch"}, float32(len(logs)))
	metrics.AddSample([]string{"raft", "sqlite3", "logBatchSize"}, float32(batchSize))
	// Both the deferral and the inline function are important for this metrics
	// accuracy. Deferral allows us to calculate the metric after the tx.Commit
	// has finished and thus account for all the processing of the operation.
	// The inlined function ensures that we do not calculate the time.Since(now)
	// at the time of deferral but rather when the go runtime executes the
	// deferred function.
	defer func() {
		metrics.AddSample([]string{"raft", "sqlite3", "writeCapacity"}, (float32(1_000_000_000)/float32(time.Since(now).Nanoseconds()))*float32(len(logs)))
		metrics.MeasureSince([]string{"raft", "sqlite3", "storeLogs"}, now)
	}()

	return tx.Commit()
}

// DeleteRange is used to delete logs within a given range inclusively.
func (b *Sqlite3Store) DeleteRange(min, max uint64) error {
	minKey := uint64ToBytes(min)
	maxKey := uint64ToBytes(max)

	_, err := b.db.Exec(`DELETE FROM logs WHERE idx >= ? AND idx <= ?`,
		minKey, maxKey)
	return err
}

// Set is used to set a key/value set outside of the raft log
func (b *Sqlite3Store) Set(k, v []byte) error {
	_, err := b.db.Exec(`INSERT INTO conf (name, value) VALUES (?, ?)
ON CONFLICT (name) DO UPDATE SET value = ?`,
		k, v, v)
	return err
}

// Get is used to retrieve a value from the k/v store by key
func (b *Sqlite3Store) Get(k []byte) ([]byte, error) {
	var val []byte
	err := b.db.QueryRow(`SELECT value FROM conf WHERE name = ?`, k).Scan(&val)
	switch {
	case err == sql.ErrNoRows:
		return nil, ErrKeyNotFound
	case err != nil:
		return nil, err
	default:
		return val, nil
	}
}

// SetUint64 is like Set, but handles uint64 values
func (b *Sqlite3Store) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

// GetUint64 is like Get, but handles uint64 values
func (b *Sqlite3Store) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

// Sync performs an fsync on the database file handle. This is not necessary
// under normal operation unless NoSync is enabled, in which this forces the
// database file to sync against the disk.
func (b *Sqlite3Store) Sync() error {
	return nil
}
