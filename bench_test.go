package raftsqlite3

import (
	"os"
	"testing"

	raftbench "github.com/hashicorp/raft/bench"
)

func BenchmarkSqlite3Store_FirstIndex(b *testing.B) {
	store := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.FirstIndex(b, store)
}

func BenchmarkSqlite3Store_LastIndex(b *testing.B) {
	store := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.LastIndex(b, store)
}

func BenchmarkSqlite3Store_GetLog(b *testing.B) {
	store := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.GetLog(b, store)
}

func BenchmarkSqlite3Store_StoreLog(b *testing.B) {
	store := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.StoreLog(b, store)
}

func BenchmarkSqlite3Store_StoreLogs(b *testing.B) {
	store := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.StoreLogs(b, store)
}

func BenchmarkSqlite3Store_DeleteRange(b *testing.B) {
	store := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.DeleteRange(b, store)
}

func BenchmarkSqlite3Store_Set(b *testing.B) {
	store := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.Set(b, store)
}

func BenchmarkSqlite3Store_Get(b *testing.B) {
	store := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.Get(b, store)
}

func BenchmarkSqlite3Store_SetUint64(b *testing.B) {
	store := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.SetUint64(b, store)
}

func BenchmarkSqlite3Store_GetUint64(b *testing.B) {
	store := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.GetUint64(b, store)
}
