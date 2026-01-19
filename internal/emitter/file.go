package emitter

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/model"
	"github.com/natefinch/lumberjack"
)

// WriterFactory creates a new WriteCloser.
type WriterFactory func(cfg config.FileEmitterConfig) (io.WriteCloser, error)

// FileOption configures the FileEmitter.
type FileOption func(*FileEmitter)

// WithWriterFactory sets a custom factory for creating the writer.
func WithWriterFactory(f WriterFactory) FileOption {
	return func(e *FileEmitter) {
		e.factory = f
	}
}

// FileEmitter writes log entries to rotating files.
type FileEmitter struct {
	cfg     config.FileEmitterConfig
	factory WriterFactory
	writer  io.WriteCloser
	mu      sync.Mutex
}

// NewFileEmitter creates a new file emitter.
func NewFileEmitter(cfg config.FileEmitterConfig, opts ...FileOption) *FileEmitter {
	e := &FileEmitter{
		cfg: cfg,
	}

	// Default factory creates lumberjack logger
	e.factory = func(cfg config.FileEmitterConfig) (io.WriteCloser, error) {
		return &lumberjack.Logger{
			Filename:   cfg.Path,
			MaxSize:    cfg.MaxSizeMB,
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAgeDays,
			Compress:   cfg.Compress,
		}, nil
	}

	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Name returns the emitter identifier.
func (f *FileEmitter) Name() string {
	return "file"
}

// Start initializes the rotating file writer.
func (f *FileEmitter) Start(ctx context.Context) error {
	w, err := f.factory(f.cfg)
	if err != nil {
		return err
	}
	f.writer = w
	return nil
}

// Stop closes the file writer.
func (f *FileEmitter) Stop(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writer != nil {
		return f.writer.Close()
	}
	return nil
}

// Emit writes a log entry to the file.
func (f *FileEmitter) Emit(ctx context.Context, entry *model.LogEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writer == nil {
		return nil
	}

	data := map[string]any{
		"timestamp": entry.Timestamp.Format(time.RFC3339Nano),
		"source":    entry.Source,
		"message":   string(entry.Raw),
	}

	for k, v := range entry.Parsed {
		data[k] = v
	}
	for k, v := range entry.Metadata {
		data[k] = v
	}

	output, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, err = f.writer.Write(append(output, '\n'))
	return err
}
