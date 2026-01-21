package emitter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/GabrielNunesIT/go-libs/logger"
	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/model"
)

// VictoriaLogsEmitter writes log entries to VictoriaLogs.
type VictoriaLogsEmitter struct {
	cfg    config.VictoriaLogsEmitterConfig
	client HTTPDoer
	batch  []map[string]any
	mu     sync.Mutex
	done   chan struct{}
	logger logger.ILogger
}

// VictoriaLogsOption configures a VictoriaLogsEmitter.
type VictoriaLogsOption func(*VictoriaLogsEmitter)

// WithVictoriaLogsHTTPClient sets a custom HTTP client for testing.
func WithVictoriaLogsHTTPClient(client HTTPDoer) VictoriaLogsOption {
	return func(v *VictoriaLogsEmitter) {
		v.client = client
	}
}

// NewVictoriaLogsEmitter creates a new VictoriaLogs emitter.
func NewVictoriaLogsEmitter(cfg config.VictoriaLogsEmitterConfig, log logger.ILogger, opts ...VictoriaLogsOption) *VictoriaLogsEmitter {
	v := &VictoriaLogsEmitter{
		cfg: cfg,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		done:   make(chan struct{}),
		logger: log.SubLogger("VictoriaLogsEmitter"),
	}
	for _, opt := range opts {
		opt(v)
	}
	return v
}

// Name returns the emitter identifier.
func (v *VictoriaLogsEmitter) Name() string {
	return "victorialogs"
}

// Start begins the background flush goroutine.
func (v *VictoriaLogsEmitter) Start(ctx context.Context) error {
	v.logger.Infof("connected to VictoriaLogs: url=%s", v.cfg.URL)
	go v.flushLoop(ctx)
	return nil
}

// Stop flushes remaining entries and shuts down.
func (v *VictoriaLogsEmitter) Stop(ctx context.Context) error {
	close(v.done)
	v.logger.Debug("flushing remaining entries")
	return v.flush(ctx)
}

// flushLoop periodically flushes the buffer.
func (v *VictoriaLogsEmitter) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(v.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-v.done:
			return
		case <-ticker.C:
			if err := v.flush(ctx); err != nil {
				v.logger.Debugf("flush error: %v", err)
			}
		}
	}
}

// Emit adds a log entry to the batch.
func (v *VictoriaLogsEmitter) Emit(ctx context.Context, entry *model.LogEntry) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Build document
	doc := map[string]any{
		"_time":   entry.Timestamp.Format(time.RFC3339Nano),
		"_msg":    string(entry.Raw),
		"_source": entry.Source,
	}

	for k, val := range entry.Parsed {
		doc[k] = val
	}
	for k, val := range entry.Metadata {
		doc[k] = val
	}

	v.batch = append(v.batch, doc)

	// Check if we should flush
	if len(v.batch) >= v.cfg.BatchSize {
		return v.flushLocked(ctx)
	}

	return nil
}

// flush sends the batch to VictoriaLogs.
func (v *VictoriaLogsEmitter) flush(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.flushLocked(ctx)
}

// flushLocked sends the batch (caller must hold lock).
func (v *VictoriaLogsEmitter) flushLocked(ctx context.Context) error {
	if len(v.batch) == 0 {
		return nil
	}

	batchSize := len(v.batch)

	// VictoriaLogs uses jsonline format
	var buf bytes.Buffer
	for _, doc := range v.batch {
		data, err := json.Marshal(doc)
		if err != nil {
			continue
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}

	url := v.cfg.URL + "/insert/jsonline"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, &buf)
	if err != nil {
		return err
	}

	httpReq.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := v.client.Do(httpReq)
	if err != nil {
		v.logger.Debugf("push failed: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		v.logger.Debugf("push failed: status=%d", resp.StatusCode)
		return fmt.Errorf("victorialogs push failed with status: %d", resp.StatusCode)
	}

	v.logger.Debugf("pushed %d entries to VictoriaLogs", batchSize)

	// Clear batch
	v.batch = v.batch[:0]
	return nil
}
