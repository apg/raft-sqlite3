package raftsqlite3

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func testSqlite3Store(t testing.TB) *Sqlite3Store {
	fh, err := ioutil.TempFile("", "sqlite3")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())

	// Successfully creates and returns a store
	store, err := New(fh.Name())
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	return store
}

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestSqlite3Store_Implements(t *testing.T) {
	var store interface{} = &Sqlite3Store{}
	if _, ok := store.(raft.StableStore); !ok {
		t.Fatalf("Sqlite3Store does not implement raft.StableStore")
	}
	if _, ok := store.(raft.LogStore); !ok {
		t.Fatalf("Sqlite3Store does not implement raft.LogStore")
	}
}

func TestNewSqlite3Store(t *testing.T) {
	fh, err := ioutil.TempFile("", "sqlite3")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())
	defer os.Remove(fh.Name())

	// Successfully creates and returns a store
	store, err := New(fh.Name(),
		WithPragmas(
			BusyTimeout(time.Millisecond*5000),
			JournalDelete,
			SynchronousNormal,
		),
	)

	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the file was created
	if store.path != fh.Name() {
		t.Fatalf("unexpected file path %q", store.path)
	}
	if _, err := os.Stat(fh.Name()); err != nil {
		t.Fatalf("err: %s", err)
	}

	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("err: %s", err)
		}
	}()

	// Check to see if PRAGMAs were set
	var value string
	for _, p := range store.pragmas {
		err := store.db.QueryRow(fmt.Sprintf("PRAGMA %s", p.Pragma())).
			Scan(&value)
		if err != nil {
			t.Fatalf("err: %s", err)
		}

		if p.String() != value {
			t.Fatalf("bad %s: %s", p.Pragma(), value)
		}
	}

	// Check to see if tables were created
	expected := map[string]bool{
		"conf": true, "logs": true,
	}

	rows, err := store.db.Query(`SELECT name FROM sqlite_master
WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
`)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			log.Fatal(err)
		}
		if _, ok := expected[name]; !ok {
			t.Fatalf("bad: %s", name)
		} else {
			delete(expected, name)
		}
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	// assert we found all tables
	if len(expected) != 0 {
		t.Fatalf("still have: %v", expected)
	}
}

func TestNewSqlite3Store_RO(t *testing.T) {
	fh, err := ioutil.TempFile("", "sqlite3")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())
	defer os.Remove(fh.Name())

	store, err := New(fh.Name())
	store.Close()

	// Successfully creates and returns a store
	store, err = New(fh.Name(), WithMode(AccessRO))
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Try to write. It should fail.
	k, v := []byte("hello"), []byte("world")

	// Try to set a k/v pair
	if err := store.Set(k, v); err == nil {
		t.Fatalf("err: %s", err)
	}
}

func TestSqlite3Store_FirstIndex(t *testing.T) {
	store := testSqlite3Store(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Should get 0 index on empty log
	idx, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the first Raft index
	idx, err = store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestSqlite3Store_LastIndex(t *testing.T) {
	store := testSqlite3Store(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Should get 0 index on empty log
	idx, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the last Raft index
	idx, err = store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestSqlite3Store_GetLog(t *testing.T) {
	store := testSqlite3Store(t)
	defer store.Close()
	defer os.Remove(store.path)

	log := new(raft.Log)

	// Should return an error on non-existent log
	if err := store.GetLog(1, log); err != raft.ErrLogNotFound {
		t.Fatalf("expected raft log not found error, got: %v", err)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Should return the proper log
	if err := store.GetLog(2, log); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(log, logs[1]) {
		t.Fatalf("bad: %#v, expected: %#v", log, logs[1])
	}
}

func TestSqlite3Store_SetLog(t *testing.T) {
	store := testSqlite3Store(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Retrieve the log again
	result := new(raft.Log)
	if err := store.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Fatalf("bad: %v", result)
	}
}

func TestSqlite3Store_SetLogs(t *testing.T) {
	store := testSqlite3Store(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure we stored them all
	result1, result2 := new(raft.Log), new(raft.Log)
	if err := store.GetLog(1, result1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[0], result1) {
		t.Fatalf("bad: %#v", result1)
	}
	if err := store.GetLog(2, result2); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[1], result2) {
		t.Fatalf("bad: %#v", result2)
	}
}

func TestSqlite3Store_DeleteRange(t *testing.T) {
	store := testSqlite3Store(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create a set of logs
	log1 := testRaftLog(1, "log1")
	log2 := testRaftLog(2, "log2")
	log3 := testRaftLog(3, "log3")
	logs := []*raft.Log{log1, log2, log3}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Attempt to delete a range of logs
	if err := store.DeleteRange(1, 2); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the logs were deleted
	if err := store.GetLog(1, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log1")
	}
	if err := store.GetLog(2, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log2")
	}
}

func TestSqlite3Store_Set_Get(t *testing.T) {
	store := testSqlite3Store(t)
	defer store.Close()
	//	defer os.Remove(store.path)

	// Returns error on non-existent key
	if _, err := store.Get([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("hello"), []byte("world")

	// Try to set a k/v pair
	if err := store.Set(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Try to read it back
	val, err := store.Get(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !bytes.Equal(val, v) {
		t.Fatalf("bad: %v", val)
	}
}

func TestSqlite3Store_SetUint64_GetUint64(t *testing.T) {
	store := testSqlite3Store(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Returns error on non-existent key
	if _, err := store.GetUint64([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("abc"), uint64(123)

	// Attempt to set the k/v pair
	if err := store.SetUint64(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Read back the value
	val, err := store.GetUint64(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if val != v {
		t.Fatalf("bad: %v", val)
	}
}
