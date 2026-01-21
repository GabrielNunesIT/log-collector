package emitter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/GabrielNunesIT/go-libs/logger"
	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/model"
)

// StdoutEmitter writes log entries to standard output.
type StdoutEmitter struct {
	cfg    config.StdoutEmitterConfig
	writer io.Writer
	mu     sync.Mutex
	logger logger.ILogger
}

// NewStdoutEmitter creates a new stdout emitter.
func NewStdoutEmitter(cfg config.StdoutEmitterConfig, log logger.ILogger) *StdoutEmitter {
	return &StdoutEmitter{
		cfg:    cfg,
		writer: os.Stdout,
		logger: log.SubLogger("StdoutEmitter"),
	}
}

// NewStdoutEmitterWithWriter creates a stdout emitter with a custom writer (for testing).
func NewStdoutEmitterWithWriter(cfg config.StdoutEmitterConfig, w io.Writer, log logger.ILogger) *StdoutEmitter {
	return &StdoutEmitter{
		cfg:    cfg,
		writer: w,
		logger: log.SubLogger("StdoutEmitter"),
	}
}

// Name returns the emitter identifier.
func (s *StdoutEmitter) Name() string {
	return "stdout"
}

// Start initializes the emitter (no-op for stdout).
func (s *StdoutEmitter) Start(ctx context.Context) error {
	s.logger.Debugf("stdout emitter started: format=%s", s.cfg.Format)
	return nil
}

// Stop gracefully shuts down the emitter (no-op for stdout).
func (s *StdoutEmitter) Stop(ctx context.Context) error {
	s.logger.Debug("stdout emitter stopped")
	return nil
}

// Emit writes a log entry to stdout.
func (s *StdoutEmitter) Emit(ctx context.Context, entry *model.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var output []byte
	var err error

	switch s.cfg.Format {
	case "json":
		output, err = s.formatJSON(entry)
	case "text":
		output, err = s.formatText(entry)
	default:
		output, err = s.formatJSON(entry)
	}

	if err != nil {
		return err
	}

	_, err = s.writer.Write(append(output, '\n'))
	return err
}

// formatJSON formats the entry as JSON.
func (s *StdoutEmitter) formatJSON(entry *model.LogEntry) ([]byte, error) {
	data := map[string]any{
		"timestamp": entry.Timestamp.Format(time.RFC3339Nano),
		"source":    entry.Source,
		"message":   string(entry.Raw),
	}

	// Merge parsed fields
	for k, v := range entry.Parsed {
		data[k] = v
	}

	// Merge metadata
	for k, v := range entry.Metadata {
		data[k] = v
	}

	return json.Marshal(data)
}

// formatText formats the entry as plain text.
func (s *StdoutEmitter) formatText(entry *model.LogEntry) ([]byte, error) {
	ts := entry.Timestamp.Format(time.RFC3339)
	msg := string(entry.Raw)
	output := fmt.Sprintf("[%s] [%s] %s", ts, entry.Source, msg)
	return []byte(output), nil
}
