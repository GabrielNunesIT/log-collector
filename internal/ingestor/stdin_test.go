package ingestor

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/model"
)

func TestStdinIngestor(t *testing.T) {
	input := "line 1\nline 2\nline 3\n"
	reader := bytes.NewBufferString(input)

	cfg := config.StdinIngestorConfig{Enabled: true}
	ingestor := NewStdinIngestorWithReader(cfg, reader)

	if ingestor.Name() != "stdin" {
		t.Errorf("expected name 'stdin', got %q", ingestor.Name())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out := make(chan *model.LogEntry, 10)

	go func() {
		err := ingestor.Start(ctx, out)
		if err != nil && err != context.Canceled {
			t.Errorf("Start failed: %v", err)
		}
	}()

	var entries []*model.LogEntry
	for entry := range out {
		entries = append(entries, entry)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	expected := []string{"line 1", "line 2", "line 3"}
	for i, entry := range entries {
		if string(entry.Raw) != expected[i] {
			t.Errorf("entry %d: expected %q, got %q", i, expected[i], string(entry.Raw))
		}
		if entry.Source != "stdin" {
			t.Errorf("entry %d: expected source 'stdin', got %q", i, entry.Source)
		}
	}
}

func TestStdinIngestor_EmptyLines(t *testing.T) {
	input := "line 1\n\nline 2\n"
	reader := bytes.NewBufferString(input)

	cfg := config.StdinIngestorConfig{Enabled: true}
	ingestor := NewStdinIngestorWithReader(cfg, reader)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out := make(chan *model.LogEntry, 10)

	go func() {
		_ = ingestor.Start(ctx, out)
	}()

	var entries []*model.LogEntry
	for entry := range out {
		entries = append(entries, entry)
	}

	// Empty lines should be skipped
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries (empty lines skipped), got %d", len(entries))
	}
}
