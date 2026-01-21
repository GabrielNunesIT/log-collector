package cli

import (
	"os"
	"strings"

	"github.com/GabrielNunesIT/go-libs/logger"
)

// SetupLogging creates and configures a logger with the specified level.
// Returns the configured logger for dependency injection.
func SetupLogging(level string) logger.ILogger {
	log := logger.NewConsoleLogger(os.Stderr)

	switch strings.ToLower(level) {
	case "trace":
		log.SetLevel(logger.LevelTrace)
	case "debug":
		log.SetLevel(logger.LevelDebug)
	case "warn", "warning":
		log.SetLevel(logger.LevelWarning)
	case "error":
		log.SetLevel(logger.LevelError)
	default:
		log.SetLevel(logger.LevelInfo)
	}

	// Set as default logger for global access if needed
	logger.SetDefaultLogger(log)
	logger.SetCtxFallbackLogger(log)

	return log
}
