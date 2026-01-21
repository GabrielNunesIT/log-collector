package testutil

import (
	"io"

	"github.com/GabrielNunesIT/go-libs/logger"
)

// NewTestLogger creates a logger that discards output, suitable for tests.
func NewTestLogger() logger.ILogger {
	return logger.NewConsoleLogger(io.Discard)
}
