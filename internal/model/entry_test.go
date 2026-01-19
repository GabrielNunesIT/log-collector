package model

import (
	"testing"
	"time"
)

func TestNewLogEntry(t *testing.T) {
	raw := []byte("test message")
	entry := NewLogEntry("test-source", raw)

	if entry.Source != "test-source" {
		t.Errorf("expected source 'test-source', got %q", entry.Source)
	}

	if string(entry.Raw) != "test message" {
		t.Errorf("expected raw 'test message', got %q", string(entry.Raw))
	}

	if entry.Parsed == nil {
		t.Error("expected Parsed map to be initialized")
	}

	if entry.Metadata == nil {
		t.Error("expected Metadata map to be initialized")
	}

	if entry.Timestamp.IsZero() {
		t.Error("expected Timestamp to be set")
	}
}

func TestLogEntry_Clone(t *testing.T) {
	original := &LogEntry{
		Timestamp: time.Now(),
		Source:    "original",
		Raw:       []byte("original message"),
		Parsed:    map[string]any{"key": "value"},
		Metadata:  map[string]string{"host": "localhost"},
	}

	clone := original.Clone()

	// Verify values are copied
	if clone.Source != original.Source {
		t.Errorf("expected source %q, got %q", original.Source, clone.Source)
	}

	if string(clone.Raw) != string(original.Raw) {
		t.Errorf("expected raw %q, got %q", string(original.Raw), string(clone.Raw))
	}

	// Verify maps are independent copies
	clone.Parsed["new"] = "field"
	if _, exists := original.Parsed["new"]; exists {
		t.Error("modifying clone.Parsed should not affect original")
	}

	clone.Metadata["new"] = "meta"
	if _, exists := original.Metadata["new"]; exists {
		t.Error("modifying clone.Metadata should not affect original")
	}

	// Verify raw slice is independent
	clone.Raw[0] = 'X'
	if original.Raw[0] == 'X' {
		t.Error("modifying clone.Raw should not affect original")
	}
}
