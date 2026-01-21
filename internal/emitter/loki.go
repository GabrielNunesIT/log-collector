package emitter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/GabrielNunesIT/go-libs/logger"
	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/model"
)

// LokiEmitter writes log entries to Grafana Loki.
type LokiEmitter struct {
	cfg    config.LokiEmitterConfig
	client HTTPDoer
	batch  []lokiStream
	mu     sync.Mutex
	done   chan struct{}
	logger logger.ILogger
}

// lokiPushRequest is the Loki push API request format.
type lokiPushRequest struct {
	Streams []lokiStream `json:"streams"`
}

// lokiStream represents a log stream in Loki.
type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

// LokiOption configures a LokiEmitter.
type LokiOption func(*LokiEmitter)

// WithLokiHTTPClient sets a custom HTTP client for testing.
func WithLokiHTTPClient(client HTTPDoer) LokiOption {
	return func(l *LokiEmitter) {
		l.client = client
	}
}

// NewLokiEmitter creates a new Loki emitter.
func NewLokiEmitter(cfg config.LokiEmitterConfig, log logger.ILogger, opts ...LokiOption) *LokiEmitter {
	l := &LokiEmitter{
		cfg: cfg,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		done:   make(chan struct{}),
		logger: log.SubLogger("LokiEmitter"),
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Name returns the emitter identifier.
func (l *LokiEmitter) Name() string {
	return "loki"
}

// Start begins the background flush goroutine.
func (l *LokiEmitter) Start(ctx context.Context) error {
	l.logger.Infof("connected to Loki: url=%s", l.cfg.URL)
	go l.flushLoop(ctx)
	return nil
}

// Stop flushes remaining entries and shuts down.
func (l *LokiEmitter) Stop(ctx context.Context) error {
	close(l.done)
	l.logger.Debug("flushing remaining entries")
	return l.flush(ctx)
}

// flushLoop periodically flushes the buffer.
func (l *LokiEmitter) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(l.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-l.done:
			return
		case <-ticker.C:
			if err := l.flush(ctx); err != nil {
				l.logger.Debugf("flush error: %v", err)
			}
		}
	}
}

// Emit adds a log entry to the batch.
func (l *LokiEmitter) Emit(ctx context.Context, entry *model.LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Build labels
	labels := make(map[string]string)
	for k, v := range l.cfg.Labels {
		labels[k] = v
	}
	labels["source"] = entry.Source

	// Add metadata as labels (Loki requires labels to be strings)
	for k, v := range entry.Metadata {
		labels[k] = v
	}

	// Build log line (include parsed fields as JSON if present)
	var line string
	if len(entry.Parsed) > 0 {
		data := map[string]any{
			"message": string(entry.Raw),
		}
		for k, v := range entry.Parsed {
			data[k] = v
		}
		jsonLine, _ := json.Marshal(data)
		line = string(jsonLine)
	} else {
		line = string(entry.Raw)
	}

	// Timestamp in nanoseconds
	ts := strconv.FormatInt(entry.Timestamp.UnixNano(), 10)

	// Find or create stream
	found := false
	for i := range l.batch {
		if l.labelsEqual(l.batch[i].Stream, labels) {
			l.batch[i].Values = append(l.batch[i].Values, []string{ts, line})
			found = true
			break
		}
	}
	if !found {
		l.batch = append(l.batch, lokiStream{
			Stream: labels,
			Values: [][]string{{ts, line}},
		})
	}

	// Check if we should flush
	if l.batchSize() >= l.cfg.BatchSize {
		return l.flushLocked(ctx)
	}

	return nil
}

// batchSize returns the total number of log lines in the batch.
func (l *LokiEmitter) batchSize() int {
	count := 0
	for _, s := range l.batch {
		count += len(s.Values)
	}
	return count
}

// labelsEqual checks if two label maps are equal.
func (l *LokiEmitter) labelsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// flush sends the batch to Loki.
func (l *LokiEmitter) flush(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.flushLocked(ctx)
}

// flushLocked sends the batch (caller must hold lock).
func (l *LokiEmitter) flushLocked(ctx context.Context) error {
	if len(l.batch) == 0 {
		return nil
	}

	batchSize := l.batchSize()
	req := lokiPushRequest{Streams: l.batch}
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}

	url := l.cfg.URL + "/loki/api/v1/push"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if l.cfg.TenantID != "" {
		httpReq.Header.Set("X-Scope-OrgID", l.cfg.TenantID)
	}

	resp, err := l.client.Do(httpReq)
	if err != nil {
		l.logger.Debugf("push failed: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		l.logger.Debugf("push failed: status=%d", resp.StatusCode)
		return fmt.Errorf("loki push failed with status: %d", resp.StatusCode)
	}

	l.logger.Debugf("pushed %d entries to Loki", batchSize)

	// Clear batch
	l.batch = l.batch[:0]
	return nil
}
