// Package emitter defines the interface and implementations for log destinations.
package emitter

import (
	"context"
	"net/http"

	"github.com/user/log-collector/internal/model"
)

// Emitter defines the contract for log destinations.
// Each emitter receives processed log entries and writes them to a destination.
type Emitter interface {
	// Start initializes the emitter (connections, buffers, etc.).
	// Called once before Emit is called.
	Start(ctx context.Context) error

	// Emit sends a log entry to the destination.
	// Must be safe to call concurrently.
	Emit(ctx context.Context, entry *model.LogEntry) error

	// Stop gracefully shuts down the emitter.
	// Should flush any buffered data before returning.
	Stop(ctx context.Context) error

	// Name returns a unique identifier for this emitter.
	Name() string
}

// HTTPDoer abstracts HTTP client operations for testing.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Ensure http.Client implements HTTPDoer.
var _ HTTPDoer = (*http.Client)(nil)
