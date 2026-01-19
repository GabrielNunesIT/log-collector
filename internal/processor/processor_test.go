package processor

import (
	"context"
	"testing"

	"github.com/user/log-collector/internal/config"
	"github.com/user/log-collector/internal/model"
)

func TestParser_JSONAutoDetect(t *testing.T) {
	cfg := config.ParserConfig{
		Enabled:        true,
		JSONAutoDetect: true,
	}

	parser, err := NewParser(cfg)
	if err != nil {
		t.Fatalf("failed to create parser: %v", err)
	}

	tests := []struct {
		name     string
		raw      string
		wantKey  string
		wantVal  any
		wantJSON bool
	}{
		{
			name:     "valid JSON object",
			raw:      `{"level":"info","msg":"test"}`,
			wantKey:  "level",
			wantVal:  "info",
			wantJSON: true,
		},
		{
			name:     "plain text",
			raw:      "just a plain log line",
			wantJSON: false,
		},
		{
			name:     "JSON with whitespace",
			raw:      `  {"key": "value"}  `,
			wantKey:  "key",
			wantVal:  "value",
			wantJSON: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := model.NewLogEntry("test", []byte(tt.raw))
			err := parser.Process(context.Background(), entry)
			if err != nil {
				t.Fatalf("Process failed: %v", err)
			}

			if tt.wantJSON {
				if entry.Parsed["_parsed_format"] != "json" {
					t.Errorf("expected _parsed_format=json, got %v", entry.Parsed["_parsed_format"])
				}
				if entry.Parsed[tt.wantKey] != tt.wantVal {
					t.Errorf("expected %s=%v, got %v", tt.wantKey, tt.wantVal, entry.Parsed[tt.wantKey])
				}
			} else {
				if _, ok := entry.Parsed["_parsed_format"]; ok {
					t.Errorf("expected no _parsed_format for non-JSON, got %v", entry.Parsed["_parsed_format"])
				}
			}
		})
	}
}

func TestParser_RegexPatterns(t *testing.T) {
	cfg := config.ParserConfig{
		Enabled:        true,
		JSONAutoDetect: false,
		Patterns: []string{
			`(?P<level>\w+): (?P<message>.+)`,
		},
	}

	parser, err := NewParser(cfg)
	if err != nil {
		t.Fatalf("failed to create parser: %v", err)
	}

	entry := model.NewLogEntry("test", []byte("INFO: application started"))
	err = parser.Process(context.Background(), entry)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if entry.Parsed["level"] != "INFO" {
		t.Errorf("expected level=INFO, got %v", entry.Parsed["level"])
	}
	if entry.Parsed["message"] != "application started" {
		t.Errorf("expected message='application started', got %v", entry.Parsed["message"])
	}
}

func TestEnricher(t *testing.T) {
	cfg := config.EnricherConfig{
		Enabled:      true,
		AddHostname:  true,
		AddTimestamp: true,
		StaticLabels: map[string]string{
			"env": "test",
		},
	}

	enricher := WithHostname(cfg, "test-host")
	entry := model.NewLogEntry("test", []byte("log line"))

	err := enricher.Process(context.Background(), entry)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if entry.Metadata["hostname"] != "test-host" {
		t.Errorf("expected hostname=test-host, got %v", entry.Metadata["hostname"])
	}

	if entry.Metadata["env"] != "test" {
		t.Errorf("expected env=test, got %v", entry.Metadata["env"])
	}

	if _, ok := entry.Metadata["processed_at"]; !ok {
		t.Error("expected processed_at to be set")
	}
}

func TestChain(t *testing.T) {
	parserCfg := config.ParserConfig{
		Enabled:        true,
		JSONAutoDetect: true,
	}
	parser, _ := NewParser(parserCfg)

	enricherCfg := config.EnricherConfig{
		Enabled:     true,
		AddHostname: true,
	}
	enricher := WithHostname(enricherCfg, "chain-test")

	chain := NewChain(parser, enricher)

	entry := model.NewLogEntry("test", []byte(`{"msg":"hello"}`))
	err := chain.Process(context.Background(), entry)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Parser should have run
	if entry.Parsed["msg"] != "hello" {
		t.Errorf("expected msg=hello from parser, got %v", entry.Parsed["msg"])
	}

	// Enricher should have run
	if entry.Metadata["hostname"] != "chain-test" {
		t.Errorf("expected hostname=chain-test from enricher, got %v", entry.Metadata["hostname"])
	}
}
