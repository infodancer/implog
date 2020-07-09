package logentry

// LogEntry refers to a generic entry in a line-based log
type LogEntry interface {
	// GetUUID reports a randomly generated UUID for this entry
	GetUUID() []byte
	// IsParseError reports whether this log entry failed to parse correctly
	IsParseError() bool
	// GetLogType reports the type of the log that this entry originated from
	GetLogType() string
}
