package logstore

import (
	"context"

	"github.com/infodancer/implog/logentry"
)

// LogStore defines an interface for storing log entries
type LogStore interface {
	// Opens a connection to the LogStore
	Open() error
	// Pings the LogStore
	Ping(ctx context.Context) error
	// Init initializes the LogStore by creating tables, etc
	Init(ctx context.Context) error
	// WriteLogEntry writes a single log entry
	WriteLogEntry(ctx context.Context, entry logentry.LogEntry) error

	// Clear removes existing data from the log store, including tables
	Clear(ctx context.Context) error
	// Close closes the log store
	Close()
}
