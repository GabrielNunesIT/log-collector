package ingestor

import (
	"bufio"
	"context"
	"io"
	"os"

	"github.com/user/log-collector/internal/config"
	"github.com/user/log-collector/internal/model"
)

// StdinIngestor reads log entries from standard input.
type StdinIngestor struct {
	cfg    config.StdinIngestorConfig
	name   string
	reader io.Reader // Allows injection for testing
}

// NewStdinIngestor creates a new stdin ingestor.
func NewStdinIngestor(cfg config.StdinIngestorConfig) *StdinIngestor {
	return &StdinIngestor{
		cfg:    cfg,
		name:   "stdin",
		reader: os.Stdin,
	}
}

// NewStdinIngestorWithReader creates a stdin ingestor with a custom reader (for testing).
func NewStdinIngestorWithReader(cfg config.StdinIngestorConfig, reader io.Reader) *StdinIngestor {
	return &StdinIngestor{
		cfg:    cfg,
		name:   "stdin",
		reader: reader,
	}
}

// Name returns the ingestor identifier.
func (s *StdinIngestor) Name() string {
	return s.name
}

// Start begins reading from stdin and sending entries to the output channel.
func (s *StdinIngestor) Start(ctx context.Context, out chan<- *model.LogEntry) error {
	defer close(out)

	scanner := bufio.NewScanner(s.reader)
	// Increase buffer for long lines
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue // Skip empty lines
		}

		// Make a copy since scanner reuses buffer
		raw := make([]byte, len(line))
		copy(raw, line)

		entry := model.NewLogEntry(s.name, raw)

		select {
		case out <- entry:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}
