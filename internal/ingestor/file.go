package ingestor

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/model"
	"github.com/fsnotify/fsnotify"
)

// FileIngestor tails files matching configured paths and emits log entries.
type FileIngestor struct {
	cfg   config.FileIngestorConfig
	name  string
}

// NewFileIngestor creates a new file tailing ingestor.
func NewFileIngestor(cfg config.FileIngestorConfig) *FileIngestor {
	return &FileIngestor{
		cfg:  cfg,
		name: "file",
	}
}

// Name returns the ingestor identifier.
func (f *FileIngestor) Name() string {
	return f.name
}

// Start begins watching and tailing files, sending entries to the output channel.
func (f *FileIngestor) Start(ctx context.Context, out chan<- *model.LogEntry) error {
	defer close(out)

	// Expand glob patterns to get actual file paths
	var files []string
	for _, pattern := range f.cfg.Paths {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return fmt.Errorf("invalid glob pattern %q: %w", pattern, err)
		}
		files = append(files, matches...)
	}

	if len(files) == 0 {
		return fmt.Errorf("no files matched patterns: %v", f.cfg.Paths)
	}

	// Filter excluded files
	files = f.filterExcluded(files)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("creating file watcher: %w", err)
	}
	defer watcher.Close()

	// Track file positions
	positions := make(map[string]int64)
	var mu sync.Mutex

	// Initial read of existing content (tail from end)
	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			continue
		}
		positions[file] = info.Size()
		if err := watcher.Add(file); err != nil {
			return fmt.Errorf("watching file %q: %w", file, err)
		}
	}

	// Also watch directories for new files
	dirs := make(map[string]struct{})
	for _, file := range files {
		dirs[filepath.Dir(file)] = struct{}{}
	}
	for dir := range dirs {
		_ = watcher.Add(dir) // Best effort for new file detection
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}

			if event.Op&fsnotify.Write == fsnotify.Write {
				mu.Lock()
				pos := positions[event.Name]
				mu.Unlock()

				newPos, err := f.readNewLines(ctx, event.Name, pos, out)
				if err != nil {
					// Log error but continue watching
					continue
				}

				mu.Lock()
				positions[event.Name] = newPos
				mu.Unlock()
			}

			// Handle file rotation (create after delete)
			if event.Op&fsnotify.Create == fsnotify.Create {
				if f.matchesPatterns(event.Name) && !f.isExcluded(event.Name) {
					mu.Lock()
					positions[event.Name] = 0
					mu.Unlock()
					_ = watcher.Add(event.Name)
				}
			}

		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			// Log error but continue
			_ = err
		}
	}
}

// readNewLines reads new content from a file starting at the given position.
func (f *FileIngestor) readNewLines(ctx context.Context, path string, pos int64, out chan<- *model.LogEntry) (int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return pos, err
	}
	defer file.Close()

	// Check if file was truncated (rotated)
	info, err := file.Stat()
	if err != nil {
		return pos, err
	}
	if info.Size() < pos {
		pos = 0 // File was truncated, read from beginning
	}

	if _, err := file.Seek(pos, io.SeekStart); err != nil {
		return pos, err
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return pos, ctx.Err()
		default:
		}

		entry := model.NewLogEntry(f.name, []byte(scanner.Text()))
		entry.Metadata["file"] = path

		select {
		case out <- entry:
		case <-ctx.Done():
			return pos, ctx.Err()
		}
	}

	newPos, _ := file.Seek(0, io.SeekCurrent)
	return newPos, scanner.Err()
}

// filterExcluded removes files matching exclude patterns.
func (f *FileIngestor) filterExcluded(files []string) []string {
	if len(f.cfg.Exclude) == 0 {
		return files
	}

	var result []string
	for _, file := range files {
		if !f.isExcluded(file) {
			result = append(result, file)
		}
	}
	return result
}

// isExcluded checks if a file matches any exclude pattern.
func (f *FileIngestor) isExcluded(file string) bool {
	for _, pattern := range f.cfg.Exclude {
		matched, _ := filepath.Match(pattern, filepath.Base(file))
		if matched {
			return true
		}
	}
	return false
}

// matchesPatterns checks if a file matches any configured path pattern.
func (f *FileIngestor) matchesPatterns(file string) bool {
	for _, pattern := range f.cfg.Paths {
		matched, _ := filepath.Match(pattern, file)
		if matched {
			return true
		}
		// Also check if it matches the directory pattern
		matched, _ = filepath.Match(pattern, filepath.Base(file))
		if matched {
			return true
		}
	}
	return false
}

// PollInterval for systems without inotify support.
var PollInterval = 250 * time.Millisecond
