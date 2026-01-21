package config

import (
	"context"
	"sync"
	"time"

	"github.com/GabrielNunesIT/go-libs/logger"
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
	logger     logger.ILogger
}

// NewConfigWatcher creates a new config file watcher.
func NewConfigWatcher(path string, log logger.ILogger) *ConfigWatcher {
	return &ConfigWatcher{
		path:     path,
		onChange: make(chan *Config, 1),
		onError:  make(chan error, 1),
		debounce: 100 * time.Millisecond,
		logger:   log.SubLogger("ConfigWatcher"),
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

	w.logger.Debugf("started watching config file: %s", w.path)
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
			w.logger.Debug("config watcher stopped")
			return

		case event, ok := <-watcher.Events:
			if !ok {
				return
			}

			// Only react to write and create events
			if event.Op&(fsnotify.Write|fsnotify.Create) == 0 {
				continue
			}

			w.logger.Debugf("config file change detected: op=%s", event.Op)

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
			w.logger.Errorf("fsnotify error: %v", err)
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
		w.logger.Errorf("failed to reload config: %v", err)
		select {
		case w.onError <- err:
		default:
		}
		return
	}

	w.mu.Lock()
	w.lastConfig = cfg
	w.mu.Unlock()

	w.logger.Infof("config reloaded: path=%s", w.path)

	select {
	case w.onChange <- cfg:
	default:
		// Channel full, drop older update
		w.logger.Warning("config change channel full, dropping update")
	}
}

// LastConfig returns the last successfully loaded config.
func (w *ConfigWatcher) LastConfig() *Config {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastConfig
}
