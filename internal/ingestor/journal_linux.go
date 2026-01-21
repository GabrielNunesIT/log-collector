//go:build linux && cgo

package ingestor

import (
	"context"
	"fmt"
	"time"

	"github.com/GabrielNunesIT/go-libs/logger"
	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/model"
	"github.com/coreos/go-systemd/v22/sdjournal"
)

// JournalIngestor reads logs from the systemd journal.
type JournalIngestor struct {
	cfg    config.JournalIngestorConfig
	name   string
	logger logger.ILogger
}

// NewJournalIngestor creates a new systemd journal ingestor.
func NewJournalIngestor(cfg config.JournalIngestorConfig, log logger.ILogger) *JournalIngestor {
	return &JournalIngestor{
		cfg:    cfg,
		name:   "journal",
		logger: log.SubLogger("JournalIngestor"),
	}
}

// Name returns the ingestor identifier.
func (j *JournalIngestor) Name() string {
	return j.name
}

// Start begins reading from the systemd journal.
func (j *JournalIngestor) Start(ctx context.Context, out chan<- *model.LogEntry) error {
	defer close(out)

	journal, err := sdjournal.NewJournal()
	if err != nil {
		return fmt.Errorf("opening journal: %w", err)
	}
	defer journal.Close()

	j.logger.Info("journal opened")

	// Filter by units if specified
	for _, unit := range j.cfg.Units {
		if err := journal.AddMatch(fmt.Sprintf("_SYSTEMD_UNIT=%s", unit)); err != nil {
			return fmt.Errorf("adding unit filter %q: %w", unit, err)
		}
		j.logger.Debugf("filtering by unit: %s", unit)
	}

	// Seek to the end to only get new entries
	if err := journal.SeekTail(); err != nil {
		return fmt.Errorf("seeking to journal tail: %w", err)
	}
	// Move back one entry so we don't miss the first new one
	if _, err := journal.Previous(); err != nil {
		return fmt.Errorf("moving to previous entry: %w", err)
	}

	j.logger.Debug("positioned at journal tail")
	entryCount := 0

	for {
		select {
		case <-ctx.Done():
			j.logger.Debugf("journal ingestor stopped: entries_read=%d", entryCount)
			return ctx.Err()
		default:
		}

		// Wait for new entries
		status := journal.Wait(sdjournal.IndefiniteWait)
		if status == sdjournal.SD_JOURNAL_NOP {
			continue
		}

		// Read all available entries
		for {
			n, err := journal.Next()
			if err != nil {
				j.logger.Errorf("reading next entry: %v", err)
				return fmt.Errorf("reading next entry: %w", err)
			}
			if n == 0 {
				break // No more entries
			}

			entry, err := j.journalEntryToLogEntry(journal)
			if err != nil {
				j.logger.Debugf("skipping malformed entry: %v", err)
				continue // Skip malformed entries
			}
			entryCount++

			select {
			case out <- entry:
			case <-ctx.Done():
				j.logger.Debugf("journal ingestor stopped: entries_read=%d", entryCount)
				return ctx.Err()
			}
		}
	}
}

// journalEntryToLogEntry converts a journal entry to a LogEntry.
func (j *JournalIngestor) journalEntryToLogEntry(journal *sdjournal.Journal) (*model.LogEntry, error) {
	jEntry, err := journal.GetEntry()
	if err != nil {
		return nil, err
	}

	// Get the message field
	message := jEntry.Fields["MESSAGE"]
	entry := model.NewLogEntry(j.name, []byte(message))

	// Copy relevant journal fields to parsed
	fieldMappings := map[string]string{
		"_SYSTEMD_UNIT":     "unit",
		"_PID":              "pid",
		"_UID":              "uid",
		"_GID":              "gid",
		"_COMM":             "command",
		"_EXE":              "executable",
		"_HOSTNAME":         "hostname",
		"PRIORITY":          "priority",
		"SYSLOG_FACILITY":   "facility",
		"SYSLOG_IDENTIFIER": "identifier",
	}

	for jField, parsedField := range fieldMappings {
		if val, ok := jEntry.Fields[jField]; ok {
			entry.Parsed[parsedField] = val
		}
	}

	// Set timestamp from journal (RealtimeTimestamp is in microseconds)
	entry.Timestamp = time.UnixMicro(int64(jEntry.RealtimeTimestamp))

	return entry, nil
}
