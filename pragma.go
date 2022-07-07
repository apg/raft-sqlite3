package raftsqlite3

import (
	"fmt"
	"strconv"
	"time"
)

type Pragmaer interface {
	Pragma() string
	fmt.Stringer
}

type BusyTimeout time.Duration

func (b BusyTimeout) Pragma() string {
	return "busy_timeout"
}

func (b BusyTimeout) String() string {
	return fmt.Sprintf("%d", time.Duration(b).Milliseconds())
}

//
type VacuumMode int

const (
	VacuumNone VacuumMode = iota
	VacuumFull
	VacuumIncremental
)

func (v VacuumMode) Pragma() string {
	return "auto_vacuum"
}

func (v VacuumMode) String() string {
	return strconv.Itoa(int(v))
}

type JournalMode int

const (
	JournalDelete JournalMode = iota
	JournalTruncate
	JournalPersist
	JournalMemory
	JournalWAL
	JournalOff
)

func (s JournalMode) Pragma() string {
	return "journal_mode"
}

func (j JournalMode) String() string {
	switch j {
	case JournalTruncate:
		return "truncate"
	case JournalPersist:
		return "persist"
	case JournalMemory:
		return "memory"
	case JournalWAL:
		return "wal"
	case JournalOff:
		return "off"
	default:
		return "delete"
	}
}

type SynchronousMode int

const (
	SynchronousOff SynchronousMode = iota
	SynchronousNormal
	SynchronousFull
	SynchronousExtra
)

func (s SynchronousMode) Pragma() string {
	return "synchronous"
}
func (s SynchronousMode) String() string {
	return strconv.Itoa(int(s))
}

type SecureDeleteMode int

const (
	SecureDeleteOff SecureDeleteMode = iota
	SecureDeleteOn
	SecureDeleteFast
)

func (s SecureDeleteMode) Pragma() string {
	return "secure_delete"
}

func (s SecureDeleteMode) String() string {
	switch s {
	case SecureDeleteOn:
		return "1"
	case SecureDeleteFast:
		return "fast"
	default:
		return "0"
	}
}
