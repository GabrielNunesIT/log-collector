// Package ingestor defines the interface and implementations for log sources.
package ingestor

import (
	"context"

	"github.com/GabrielNunesIT/log-collector/internal/model"
)

// Ingestor defines the contract for log sources.
// Each ingestor runs in its own goroutine and pushes log entries to the output channel.
type Ingestor interface {
	// Start begins ingesting logs and sends them to the output channel.
	// It blocks until the context is cancelled or an unrecoverable error occurs.
	// The implementation must close the output channel when done.
	Start(ctx context.Context, out chan<- *model.LogEntry) error

	// Name returns a unique identifier for this ingestor instance.
	Name() string
}
