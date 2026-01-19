package processor

import (
	"context"
	"os"
	"time"

	"github.com/user/log-collector/internal/config"
	"github.com/user/log-collector/internal/model"
)

// Enricher adds metadata to log entries.
type Enricher struct {
	cfg      config.EnricherConfig
	hostname string
}

// NewEnricher creates a new enrichment processor.
func NewEnricher(cfg config.EnricherConfig) *Enricher {
	e := &Enricher{cfg: cfg}

	// Pre-fetch hostname
	if cfg.AddHostname {
		e.hostname, _ = os.Hostname()
	}

	return e
}

// Name returns the processor identifier.
func (e *Enricher) Name() string {
	return "enricher"
}

// Process enriches the log entry with additional metadata.
func (e *Enricher) Process(ctx context.Context, entry *model.LogEntry) error {
	if !e.cfg.Enabled {
		return nil
	}

	// Add hostname
	if e.cfg.AddHostname && e.hostname != "" {
		entry.Metadata["hostname"] = e.hostname
	}

	// Add processing timestamp
	if e.cfg.AddTimestamp {
		entry.Metadata["processed_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	}

	// Add static labels
	for k, v := range e.cfg.StaticLabels {
		entry.Metadata[k] = v
	}

	return nil
}

// WithHostname creates an Enricher that adds a specific hostname.
// Useful for testing or when overriding the detected hostname.
func WithHostname(cfg config.EnricherConfig, hostname string) *Enricher {
	e := NewEnricher(cfg)
	e.hostname = hostname
	return e
}
