package emitter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/user/log-collector/internal/config"
	"github.com/user/log-collector/internal/model"
)

// IndexerFactory creates a new BulkIndexer.
type IndexerFactory func(cfg config.ElasticsearchEmitterConfig) (esutil.BulkIndexer, error)

// ElasticsearchOption configures the ElasticsearchEmitter.
type ElasticsearchOption func(*ElasticsearchEmitter)

// WithIndexerFactory sets a custom factory for creating the BulkIndexer.
// This is primarily used for testing to inject a mock indexer.
func WithIndexerFactory(f IndexerFactory) ElasticsearchOption {
	return func(e *ElasticsearchEmitter) {
		e.factory = f
	}
}

// ElasticsearchEmitter writes log entries to Elasticsearch.
type ElasticsearchEmitter struct {
	cfg     config.ElasticsearchEmitterConfig
	factory IndexerFactory
	indexer esutil.BulkIndexer
	mu      sync.Mutex
}

// NewElasticsearchEmitter creates a new Elasticsearch emitter.
func NewElasticsearchEmitter(cfg config.ElasticsearchEmitterConfig, opts ...ElasticsearchOption) *ElasticsearchEmitter {
	e := &ElasticsearchEmitter{
		cfg: cfg,
	}

	// Default factory creates real client and indexer
	e.factory = func(cfg config.ElasticsearchEmitterConfig) (esutil.BulkIndexer, error) {
		esCfg := elasticsearch.Config{
			Addresses: cfg.Addresses,
		}

		if cfg.Username != "" {
			esCfg.Username = cfg.Username
			esCfg.Password = cfg.Password
		}

		client, err := elasticsearch.NewClient(esCfg)
		if err != nil {
			return nil, fmt.Errorf("creating elasticsearch client: %w", err)
		}

		return esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
			Client:        client,
			Index:         cfg.Index,
			NumWorkers:    2,
			FlushBytes:    5e+6, // 5MB
			FlushInterval: cfg.FlushInterval,
		})
	}

	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Name returns the emitter identifier.
func (e *ElasticsearchEmitter) Name() string {
	return "elasticsearch"
}

// Start initializes the Elasticsearch client and bulk indexer.
func (e *ElasticsearchEmitter) Start(ctx context.Context) error {
	indexer, err := e.factory(e.cfg)
	if err != nil {
		return err
	}
	e.indexer = indexer
	return nil
}

// Stop flushes and closes the bulk indexer.
func (e *ElasticsearchEmitter) Stop(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.indexer != nil {
		return e.indexer.Close(ctx)
	}
	return nil
}

// Emit adds a log entry to the bulk indexer.
func (e *ElasticsearchEmitter) Emit(ctx context.Context, entry *model.LogEntry) error {
	doc := map[string]any{
		"@timestamp": entry.Timestamp.Format(time.RFC3339Nano),
		"source":     entry.Source,
		"message":    string(entry.Raw),
	}

	for k, v := range entry.Parsed {
		doc[k] = v
	}
	for k, v := range entry.Metadata {
		doc[k] = v
	}

	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	return e.indexer.Add(ctx, esutil.BulkIndexerItem{
		Action: "index",
		Body:   bytes.NewReader(data),
		OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
			// Log failure but don't block
		},
	})
}
