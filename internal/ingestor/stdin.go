package ingestor

import (
	"bufio"
	"context"
	"io"
	"os"

	"github.com/GabrielNunesIT/go-libs/logger"
	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/model"
)

// StdinIngestor reads log entries from standard input.
type StdinIngestor struct {
	cfg    config.StdinIngestorConfig
	name   string
	reader io.Reader // Allows injection for testing
	logger logger.ILogger
}

// NewStdinIngestor creates a new stdin ingestor.
func NewStdinIngestor(cfg config.StdinIngestorConfig, log logger.ILogger) *StdinIngestor {
	return &StdinIngestor{
		cfg:    cfg,
		name:   "stdin",
		reader: os.Stdin,
		logger: log.SubLogger("StdinIngestor"),
	}
}

// NewStdinIngestorWithReader creates a stdin ingestor with a custom reader (for testing).
func NewStdinIngestorWithReader(cfg config.StdinIngestorConfig, reader io.Reader, log logger.ILogger) *StdinIngestor {
	return &StdinIngestor{
		cfg:    cfg,
		name:   "stdin",
		reader: reader,
		logger: log.SubLogger("StdinIngestor"),
	}
}

// Name returns the ingestor identifier.
func (s *StdinIngestor) Name() string {
	return s.name
}

// Start begins reading from stdin and sending entries to the output channel.
func (s *StdinIngestor) Start(ctx context.Context, out chan<- *model.LogEntry) error {
	defer close(out)

	s.logger.Info("reading from stdin")

	scanner := bufio.NewScanner(s.reader)
	// Increase buffer for long lines
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	lineCount := 0
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			s.logger.Debugf("stdin ingestor stopped: lines_read=%d", lineCount)
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
		lineCount++

		select {
		case out <- entry:
		case <-ctx.Done():
			s.logger.Debugf("stdin ingestor stopped: lines_read=%d", lineCount)
			return ctx.Err()
		}
	}

	if err := scanner.Err(); err != nil {
		s.logger.Errorf("stdin read error: %v", err)
		return err
	}

	s.logger.Infof("EOF reached: lines_read=%d", lineCount)
	return nil
}
