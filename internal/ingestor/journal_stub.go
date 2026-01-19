//go:build !linux || !cgo

package ingestor

import (
	"context"
	"fmt"
	"runtime"

	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/model"
)

// JournalIngestor is a stub for non-Linux systems.
type JournalIngestor struct {
	cfg  config.JournalIngestorConfig
	name string
}

// NewJournalIngestor creates a new journal ingestor stub.
func NewJournalIngestor(cfg config.JournalIngestorConfig) *JournalIngestor {
	return &JournalIngestor{
		cfg:  cfg,
		name: "journal",
	}
}

// Name returns the ingestor identifier.
func (j *JournalIngestor) Name() string {
	return j.name
}

// Start returns an error on non-Linux systems.
func (j *JournalIngestor) Start(ctx context.Context, out chan<- *model.LogEntry) error {
	defer close(out)
	return fmt.Errorf("journal ingestor is only supported on Linux (current OS: %s)", runtime.GOOS)
}
