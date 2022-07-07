package raftsqlite3

import (
	"encoding/binary"
	"time"
)

// Converts bytes to an integer
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Converts a uint to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func timeToNanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixNano()
}

func nanosToTime(nanos int64) time.Time {
	var t time.Time
	if nanos == 0 {
		return t
	}
	return time.Unix(nanos/1000000000, nanos%1000000000).UTC()
}
