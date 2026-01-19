package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	// Ensure no config file affects the test
	origDir, _ := os.Getwd()
	tmpDir := t.TempDir()
	_ = os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Verify default values
	if cfg.LogLevel != "info" {
		t.Errorf("expected loglevel=info, got %s", cfg.LogLevel)
	}
	if cfg.Pipeline.BufferSize != 1000 {
		t.Errorf("expected buffersize=1000, got %d", cfg.Pipeline.BufferSize)
	}
	if cfg.Pipeline.ShutdownTimeout != 30*time.Second {
		t.Errorf("expected shutdowntimeout=30s, got %v", cfg.Pipeline.ShutdownTimeout)
	}
	if cfg.Pipeline.DropOnFullBuffer != false {
		t.Error("expected droponbufferfull=false")
	}

	// Emitter defaults
	if cfg.Emitters.Stdout.Enabled != true {
		t.Error("expected stdout emitter enabled by default")
	}
	if cfg.Emitters.Stdout.Format != "json" {
		t.Errorf("expected stdout format=json, got %s", cfg.Emitters.Stdout.Format)
	}

	// Ingestor defaults - all disabled
	if cfg.Ingestors.File.Enabled {
		t.Error("expected file ingestor disabled by default")
	}
	if cfg.Ingestors.Syslog.Enabled {
		t.Error("expected syslog ingestor disabled by default")
	}
	if cfg.Ingestors.Journal.Enabled {
		t.Error("expected journal ingestor disabled by default")
	}
	if cfg.Ingestors.Stdin.Enabled {
		t.Error("expected stdin ingestor disabled by default")
	}
}

func TestLoad_EnvOverride(t *testing.T) {
	origDir, _ := os.Getwd()
	tmpDir := t.TempDir()
	_ = os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	// LOG_COLLECTOR_LOGLEVEL -> loglevel
	os.Setenv("LOG_COLLECTOR_LOGLEVEL", "debug")
	defer os.Unsetenv("LOG_COLLECTOR_LOGLEVEL")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.LogLevel != "debug" {
		t.Errorf("expected loglevel=debug from env, got %s", cfg.LogLevel)
	}
}

func TestLoad_NestedEnvOverride(t *testing.T) {
	origDir, _ := os.Getwd()
	tmpDir := t.TempDir()
	_ = os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	// LOG_COLLECTOR_PIPELINE_BUFFERSIZE -> pipeline.buffersize
	os.Setenv("LOG_COLLECTOR_PIPELINE_BUFFERSIZE", "2000")
	defer os.Unsetenv("LOG_COLLECTOR_PIPELINE_BUFFERSIZE")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Pipeline.BufferSize != 2000 {
		t.Errorf("expected buffersize=2000 from nested env, got %d", cfg.Pipeline.BufferSize)
	}
}

func TestLoad_ConfigFile(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	configContent := `
loglevel: warn
pipeline:
  buffersize: 500
ingestors:
  stdin:
    enabled: true
emitters:
  stdout:
    enabled: false
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.LogLevel != "warn" {
		t.Errorf("expected loglevel=warn from file, got %s", cfg.LogLevel)
	}
	if cfg.Pipeline.BufferSize != 500 {
		t.Errorf("expected buffersize=500 from file, got %d", cfg.Pipeline.BufferSize)
	}
	if !cfg.Ingestors.Stdin.Enabled {
		t.Error("expected stdin ingestor enabled from file")
	}
	if cfg.Emitters.Stdout.Enabled {
		t.Error("expected stdout emitter disabled from file")
	}
}

func TestLoad_EnvOverridesFile(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	configContent := `loglevel: warn`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	os.Setenv("LOG_COLLECTOR_LOGLEVEL", "error")
	defer os.Unsetenv("LOG_COLLECTOR_LOGLEVEL")

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.LogLevel != "error" {
		t.Errorf("expected env to override file, got %s", cfg.LogLevel)
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	invalidContent := `
loglevel: info
  invalid_indent: true
`
	if err := os.WriteFile(configPath, []byte(invalidContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	_, err := Load(configPath)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestLoad_JSONFile(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	configContent := `{
  "loglevel": "error",
  "pipeline": {
    "buffersize": 250
  }
}`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.LogLevel != "error" {
		t.Errorf("expected loglevel=error from JSON file, got %s", cfg.LogLevel)
	}
	if cfg.Pipeline.BufferSize != 250 {
		t.Errorf("expected buffersize=250 from JSON file, got %d", cfg.Pipeline.BufferSize)
	}
}

func TestDefaults_ProcessorChainDefaults(t *testing.T) {
	d := defaults()

	// File ingestor processor defaults
	if !d.Ingestors.File.Processor.Parser.Enabled {
		t.Error("expected file parser enabled by default")
	}
	if !d.Ingestors.File.Processor.Parser.JSONAutoDetect {
		t.Error("expected file parser jsonautodetect enabled by default")
	}
	if !d.Ingestors.File.Processor.Enricher.Enabled {
		t.Error("expected file enricher enabled by default")
	}
	if !d.Ingestors.File.Processor.Enricher.AddHostname {
		t.Error("expected file enricher addhostname enabled by default")
	}
	if !d.Ingestors.File.Processor.Enricher.AddTimestamp {
		t.Error("expected file enricher addtimestamp enabled by default")
	}

	// Syslog defaults
	if d.Ingestors.Syslog.Protocol != "udp" {
		t.Errorf("expected syslog protocol=udp, got %s", d.Ingestors.Syslog.Protocol)
	}
	if d.Ingestors.Syslog.Address != ":514" {
		t.Errorf("expected syslog address=:514, got %s", d.Ingestors.Syslog.Address)
	}

	// File emitter defaults
	if d.Emitters.File.MaxSizeMB != 100 {
		t.Errorf("expected file maxsizemb=100, got %d", d.Emitters.File.MaxSizeMB)
	}
	if d.Emitters.File.MaxBackups != 3 {
		t.Errorf("expected file maxbackups=3, got %d", d.Emitters.File.MaxBackups)
	}
	if !d.Emitters.File.Compress {
		t.Error("expected file compress enabled by default")
	}

	// Batch emitter defaults
	if d.Emitters.Elasticsearch.BatchSize != 100 {
		t.Errorf("expected elasticsearch batchsize=100, got %d", d.Emitters.Elasticsearch.BatchSize)
	}
	if d.Emitters.Loki.FlushInterval != 1*time.Second {
		t.Errorf("expected loki flushinterval=1s, got %v", d.Emitters.Loki.FlushInterval)
	}
}

func TestDefaults_AllEmitterDefaults(t *testing.T) {
	d := defaults()

	if d.Emitters.VictoriaLogs.BatchSize != 100 {
		t.Errorf("expected victorialogs batchsize=100, got %d", d.Emitters.VictoriaLogs.BatchSize)
	}
	if d.Emitters.VictoriaLogs.FlushInterval != 1*time.Second {
		t.Errorf("expected victorialogs flushinterval=1s, got %v", d.Emitters.VictoriaLogs.FlushInterval)
	}
	if d.Emitters.Elasticsearch.FlushInterval != 5*time.Second {
		t.Errorf("expected elasticsearch flushinterval=5s, got %v", d.Emitters.Elasticsearch.FlushInterval)
	}
}

func TestDefaults_AllIngestorProcessorDefaults(t *testing.T) {
	d := defaults()

	if !d.Ingestors.Stdin.Processor.Parser.JSONAutoDetect {
		t.Error("expected stdin parser jsonautodetect enabled")
	}
	if !d.Ingestors.Syslog.Processor.Parser.Enabled {
		t.Error("expected syslog parser enabled")
	}
	if !d.Ingestors.Syslog.Processor.Enricher.Enabled {
		t.Error("expected syslog enricher enabled")
	}
	if !d.Ingestors.Journal.Processor.Parser.Enabled {
		t.Error("expected journal parser enabled")
	}
}

func TestLoad_DeeplyNestedEnv(t *testing.T) {
	origDir, _ := os.Getwd()
	tmpDir := t.TempDir()
	_ = os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	// LOG_COLLECTOR_INGESTORS_STDIN_ENABLED -> ingestors.stdin.enabled
	os.Setenv("LOG_COLLECTOR_INGESTORS_STDIN_ENABLED", "true")
	defer os.Unsetenv("LOG_COLLECTOR_INGESTORS_STDIN_ENABLED")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if !cfg.Ingestors.Stdin.Enabled {
		t.Error("expected stdin enabled from deeply nested env")
	}
}
