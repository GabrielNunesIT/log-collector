// Package model defines the core data structures used throughout the log collector.
package model

import (
	"time"
)

// LogEntry represents a single log event flowing through the pipeline.
// It carries both the raw log data and any parsed/enriched metadata.
type LogEntry struct {
	// Timestamp is when the log entry was ingested.
	Timestamp time.Time

	// Source identifies which ingestor produced this entry.
	Source string

	// Raw contains the original log line as received.
	Raw []byte

	// Parsed holds structured fields extracted during processing.
	// Keys are field names, values can be any JSON-compatible type.
	Parsed map[string]any

	// Metadata contains enrichment data like hostname, environment labels, etc.
	Metadata map[string]string
}

// NewLogEntry creates a new LogEntry with initialized maps and current timestamp.
func NewLogEntry(source string, raw []byte) *LogEntry {
	return &LogEntry{
		Timestamp: time.Now(),
		Source:    source,
		Raw:       raw,
		Parsed:    make(map[string]any),
		Metadata:  make(map[string]string),
	}
}

// Clone creates a deep copy of the LogEntry.
// Useful when fan-out requires independent copies for each emitter.
func (e *LogEntry) Clone() *LogEntry {
	clone := &LogEntry{
		Timestamp: e.Timestamp,
		Source:    e.Source,
		Raw:       make([]byte, len(e.Raw)),
		Parsed:    make(map[string]any, len(e.Parsed)),
		Metadata:  make(map[string]string, len(e.Metadata)),
	}
	copy(clone.Raw, e.Raw)
	for k, v := range e.Parsed {
		clone.Parsed[k] = v
	}
	for k, v := range e.Metadata {
		clone.Metadata[k] = v
	}
	return clone
}
