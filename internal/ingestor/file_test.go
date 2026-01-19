package ingestor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/user/log-collector/internal/config"
	"github.com/user/log-collector/internal/model"
)

func TestFileIngestor(t *testing.T) {
	// Create temp dir
	tmpDir, err := os.MkdirTemp("", "log-collector-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logFile := filepath.Join(tmpDir, "app.log")
	
	// Create initial file
	f, err := os.Create(logFile)
	require.NoError(t, err)
	_, _ = f.WriteString("line 1\n")
	f.Close()

	cfg := config.FileIngestorConfig{
		Enabled: true,
		Paths:   []string{filepath.Join(tmpDir, "*.log")},
	}

	ingestor := NewFileIngestor(cfg)
	out := make(chan *model.LogEntry, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start ingestor
	go func() {
		_ = ingestor.Start(ctx, out)
	}()

	// Wait for startup/initial read (which tails from end, so it might skip "line 1"?)
	// Implementation details: "Initial read of existing content (tail from end)" -> yes, positions[file] = info.Size()
	// So "line 1" is skipped.
	time.Sleep(100 * time.Millisecond)

	// Append new line
	f, err = os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.WriteString("line 2\n")
	require.NoError(t, err)
	f.Close()

	// Verify receipt
	select {
	case entry := <-out:
		assert.Equal(t, "line 2", string(entry.Raw))
		assert.Contains(t, entry.Metadata["file"], "app.log")
	case <-time.After(2 * time.Second): // File system events can be slow
		t.Fatal("timeout waiting for log entry")
	}

	// Test Rotation (Move and Recreate)
	rotatedLog := logFile + ".1"
	err = os.Rename(logFile, rotatedLog)
	require.NoError(t, err)

	// Create new file
	f, err = os.Create(logFile)
	require.NoError(t, err)
	_, err = f.WriteString("line 3\n")
	require.NoError(t, err)
	f.Close()

	select {
	case entry := <-out:
		assert.Equal(t, "line 3", string(entry.Raw))
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for rotated log entry")
	}
}

func TestFileIngestor_Exclude(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "log-collector-exclude")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.FileIngestorConfig{
		Enabled: true,
		Paths:   []string{filepath.Join(tmpDir, "*.log")},
		Exclude: []string{"*.exclude.log"},
	}

	ingestor := NewFileIngestor(cfg)
	
	assert.True(t, ingestor.isExcluded(filepath.Join(tmpDir, "test.exclude.log")))
	assert.False(t, ingestor.isExcluded(filepath.Join(tmpDir, "test.log")))
}
