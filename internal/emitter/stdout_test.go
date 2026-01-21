package emitter

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/model"
)

func TestStdoutEmitter_JSON(t *testing.T) {
	var buf bytes.Buffer
	cfg := config.StdoutEmitterConfig{
		Enabled: true,
		Format:  "json",
	}

	emitter := NewStdoutEmitterWithWriter(cfg, &buf, testLogger())

	if emitter.Name() != "stdout" {
		t.Errorf("expected name 'stdout', got %q", emitter.Name())
	}

	entry := &model.LogEntry{
		Timestamp: time.Date(2026, 1, 18, 12, 0, 0, 0, time.UTC),
		Source:    "test",
		Raw:       []byte("test message"),
		Parsed:    map[string]any{"level": "info"},
		Metadata:  map[string]string{"host": "localhost"},
	}

	err := emitter.Emit(context.Background(), entry)
	if err != nil {
		t.Fatalf("Emit failed: %v", err)
	}

	output := buf.String()
	if output == "" {
		t.Fatal("expected output, got empty string")
	}

	// Parse JSON output
	var result map[string]any
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}

	if result["source"] != "test" {
		t.Errorf("expected source=test, got %v", result["source"])
	}
	if result["level"] != "info" {
		t.Errorf("expected level=info, got %v", result["level"])
	}
	if result["host"] != "localhost" {
		t.Errorf("expected host=localhost, got %v", result["host"])
	}
}

func TestStdoutEmitter_Text(t *testing.T) {
	var buf bytes.Buffer
	cfg := config.StdoutEmitterConfig{
		Enabled: true,
		Format:  "text",
	}

	emitter := NewStdoutEmitterWithWriter(cfg, &buf, testLogger())

	entry := &model.LogEntry{
		Timestamp: time.Date(2026, 1, 18, 12, 0, 0, 0, time.UTC),
		Source:    "test",
		Raw:       []byte("test message"),
		Parsed:    map[string]any{},
		Metadata:  map[string]string{},
	}

	err := emitter.Emit(context.Background(), entry)
	if err != nil {
		t.Fatalf("Emit failed: %v", err)
	}

	output := buf.String()
	if output == "" {
		t.Fatal("expected output, got empty string")
	}

	// Text format should contain timestamp, source, and message
	if !bytes.Contains([]byte(output), []byte("[test]")) {
		t.Errorf("output should contain [test], got: %s", output)
	}
	if !bytes.Contains([]byte(output), []byte("test message")) {
		t.Errorf("output should contain 'test message', got: %s", output)
	}
}
