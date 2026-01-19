package config

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// ConfigWatcher watches a config file for changes and reloads it.
type ConfigWatcher struct {
	path       string
	onChange   chan *Config
	onError    chan error
	debounce   time.Duration
	lastConfig *Config
	mu         sync.Mutex
}

// NewConfigWatcher creates a new config file watcher.
func NewConfigWatcher(path string) *ConfigWatcher {
	return &ConfigWatcher{
		path:     path,
		onChange: make(chan *Config, 1),
		onError:  make(chan error, 1),
		debounce: 100 * time.Millisecond,
	}
}

// Changes returns channel that receives new configs on file changes.
func (w *ConfigWatcher) Changes() <-chan *Config {
	return w.onChange
}

// Errors returns channel that receives errors during reload.
func (w *ConfigWatcher) Errors() <-chan error {
	return w.onError
}

// Start begins watching the config file.
func (w *ConfigWatcher) Start(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	if err := watcher.Add(w.path); err != nil {
		watcher.Close()
		return err
	}

	go w.watchLoop(ctx, watcher)
	return nil
}

// watchLoop handles file system events.
func (w *ConfigWatcher) watchLoop(ctx context.Context, watcher *fsnotify.Watcher) {
	defer watcher.Close()

	var debounceTimer *time.Timer
	var debounceChan <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			return

		case event, ok := <-watcher.Events:
			if !ok {
				return
			}

			// Only react to write and create events
			if event.Op&(fsnotify.Write|fsnotify.Create) == 0 {
				continue
			}

			// Debounce rapid changes
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			debounceTimer = time.NewTimer(w.debounce)
			debounceChan = debounceTimer.C

		case <-debounceChan:
			debounceChan = nil
			w.reload()

		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			select {
			case w.onError <- err:
			default:
			}
		}
	}
}

// reload loads the config file and sends it on the change channel.
func (w *ConfigWatcher) reload() {
	cfg, err := Load(w.path)
	if err != nil {
		slog.Error("failed to reload config", "error", err)
		select {
		case w.onError <- err:
		default:
		}
		return
	}

	w.mu.Lock()
	w.lastConfig = cfg
	w.mu.Unlock()

	slog.Info("config reloaded", "path", w.path)

	select {
	case w.onChange <- cfg:
	default:
		// Channel full, drop older update
	}
}

// LastConfig returns the last successfully loaded config.
func (w *ConfigWatcher) LastConfig() *Config {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastConfig
}
