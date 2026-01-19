package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/user/log-collector/internal/config"
	"github.com/user/log-collector/internal/model"
)

// mockEmitter implements emitter.Emitter for testing.
type mockEmitter struct {
	name     string
	mu       sync.Mutex
	received []*model.LogEntry
	started  bool
	stopped  bool
}

func (m *mockEmitter) Name() string { return m.name }

func (m *mockEmitter) Start(ctx context.Context) error {
	m.started = true
	return nil
}

func (m *mockEmitter) Stop(ctx context.Context) error {
	m.stopped = true
	return nil
}

func (m *mockEmitter) Emit(ctx context.Context, entry *model.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.received = append(m.received, entry)
	return nil
}

func (m *mockEmitter) getReceived() []*model.LogEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*model.LogEntry, len(m.received))
	copy(result, m.received)
	return result
}

func TestPipeline_New_NoIngestors(t *testing.T) {
	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			BufferSize: 100,
		},
		// All ingestors disabled by default
		Emitters: config.EmitterConfig{
			Stdout: config.StdoutEmitterConfig{Enabled: true},
		},
	}

	_, err := New(cfg)
	if err == nil {
		t.Fatal("expected error when no ingestors enabled")
	}
}

func TestPipeline_New_NoEmitters(t *testing.T) {
	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			BufferSize: 100,
		},
		Ingestors: config.IngestorConfig{
			Stdin: config.StdinIngestorConfig{Enabled: true},
		},
		// All emitters disabled
	}

	_, err := New(cfg)
	if err == nil {
		t.Fatal("expected error when no emitters enabled")
	}
}

func TestPipeline_IngestorCount(t *testing.T) {
	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			BufferSize: 100,
		},
		Ingestors: config.IngestorConfig{
			Stdin: config.StdinIngestorConfig{Enabled: true},
		},
		Emitters: config.EmitterConfig{
			Stdout: config.StdoutEmitterConfig{Enabled: true},
		},
	}

	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if p.IngestorCount() != 1 {
		t.Errorf("expected 1 ingestor, got %d", p.IngestorCount())
	}
}

func TestPipeline_EmitterCount(t *testing.T) {
	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			BufferSize: 100,
		},
		Ingestors: config.IngestorConfig{
			Stdin: config.StdinIngestorConfig{Enabled: true},
		},
		Emitters: config.EmitterConfig{
			Stdout: config.StdoutEmitterConfig{Enabled: true},
		},
	}

	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if p.EmitterCount() != 1 {
		t.Errorf("expected 1 emitter, got %d", p.EmitterCount())
	}
}

func TestPipeline_MultipleIngestors(t *testing.T) {
	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			BufferSize: 100,
		},
		Ingestors: config.IngestorConfig{
			Stdin: config.StdinIngestorConfig{Enabled: true},
			File: config.FileIngestorConfig{
				Enabled: true,
				Paths:   []string{"/tmp/test.log"},
			},
		},
		Emitters: config.EmitterConfig{
			Stdout: config.StdoutEmitterConfig{Enabled: true},
		},
	}

	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if p.IngestorCount() != 2 {
		t.Errorf("expected 2 ingestors, got %d", p.IngestorCount())
	}
}

func TestPipeline_MultipleEmitters(t *testing.T) {
	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			BufferSize: 100,
		},
		Ingestors: config.IngestorConfig{
			Stdin: config.StdinIngestorConfig{Enabled: true},
		},
		Emitters: config.EmitterConfig{
			Stdout: config.StdoutEmitterConfig{Enabled: true},
			Loki: config.LokiEmitterConfig{
				Enabled:       true,
				URL:           "http://localhost:3100",
				FlushInterval: time.Second,
			},
		},
	}

	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if p.EmitterCount() != 2 {
		t.Errorf("expected 2 emitters, got %d", p.EmitterCount())
	}
}

func TestPipeline_BuildProcessorChain(t *testing.T) {
	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			BufferSize: 100,
		},
		Ingestors: config.IngestorConfig{
			Stdin: config.StdinIngestorConfig{
				Enabled: true,
				Processor: config.ProcessorConfig{
					Parser: config.ParserConfig{
						Enabled:        true,
						JSONAutoDetect: true,
					},
					Enricher: config.EnricherConfig{
						Enabled:     true,
						AddHostname: true,
					},
				},
			},
		},
		Emitters: config.EmitterConfig{
			Stdout: config.StdoutEmitterConfig{Enabled: true},
		},
	}

	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Verify pipeline was created with processor chain
	if p.IngestorCount() != 1 {
		t.Errorf("expected 1 ingestor with processor chain, got %d", p.IngestorCount())
	}
}
