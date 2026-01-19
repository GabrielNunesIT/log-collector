// Package config provides configuration loading with layered overrides.
// Load order: defaults -> YAML file -> environment variables.
package config

import (
	"os"
	"time"

	configloader "github.com/GabrielNunesIT/go-libs/config-loader"
)

// Config is the root configuration structure for the log collector.
type Config struct {
	LogLevel  string         `koanf:"loglevel" yaml:"log_level" json:"log_level"`
	Pipeline  PipelineConfig `koanf:"pipeline"`
	Ingestors IngestorConfig `koanf:"ingestors"`
	Emitters  EmitterConfig  `koanf:"emitters"`
}

// PipelineConfig controls the pipeline behavior.
type PipelineConfig struct {
	BufferSize       int           `koanf:"buffersize" yaml:"buffer_size" json:"buffer_size"`
	ShutdownTimeout  time.Duration `koanf:"shutdowntimeout" yaml:"shutdown_timeout" json:"shutdown_timeout"`
	DropOnFullBuffer bool          `koanf:"droponbufferfull" yaml:"drop_on_full_buffer" json:"drop_on_full_buffer"`
}

// IngestorConfig holds configuration for all ingestors.
type IngestorConfig struct {
	File    FileIngestorConfig    `koanf:"file"`
	Syslog  SyslogIngestorConfig  `koanf:"syslog"`
	Journal JournalIngestorConfig `koanf:"journal"`
	Stdin   StdinIngestorConfig   `koanf:"stdin"`
}

// FileIngestorConfig configures the file tailing ingestor.
type FileIngestorConfig struct {
	Enabled   bool            `koanf:"enabled"`
	Paths     []string        `koanf:"paths"`
	Exclude   []string        `koanf:"exclude"`
	Processor ProcessorConfig `koanf:"processor"`
}

// SyslogIngestorConfig configures the syslog ingestor.
type SyslogIngestorConfig struct {
	Enabled   bool            `koanf:"enabled"`
	Protocol  string          `koanf:"protocol"` // "udp" or "tcp"
	Address   string          `koanf:"address"`
	Processor ProcessorConfig `koanf:"processor"`
}

// JournalIngestorConfig configures the systemd journal ingestor.
type JournalIngestorConfig struct {
	Enabled   bool            `koanf:"enabled"`
	Units     []string        `koanf:"units"`
	Processor ProcessorConfig `koanf:"processor"`
}

// StdinIngestorConfig configures the stdin ingestor.
type StdinIngestorConfig struct {
	Enabled   bool            `koanf:"enabled"`
	Processor ProcessorConfig `koanf:"processor"`
}

// ProcessorConfig holds the processor chain configuration per ingestor.
type ProcessorConfig struct {
	Parser   ParserConfig   `koanf:"parser"`
	Enricher EnricherConfig `koanf:"enricher"`
}

// ParserConfig configures the parsing processor.
type ParserConfig struct {
	Enabled        bool     `koanf:"enabled"`
	JSONAutoDetect bool     `koanf:"jsonautodetect" yaml:"json_auto_detect" json:"json_auto_detect"`
	Patterns       []string `koanf:"patterns"` // Regex patterns with named groups
}

// EnricherConfig configures the enrichment processor.
type EnricherConfig struct {
	Enabled      bool              `koanf:"enabled"`
	AddHostname  bool              `koanf:"addhostname" yaml:"add_hostname" json:"add_hostname"`
	AddTimestamp bool              `koanf:"addtimestamp" yaml:"add_timestamp" json:"add_timestamp"`
	StaticLabels map[string]string `koanf:"staticlabels" yaml:"static_labels" json:"static_labels"`
}

// EmitterConfig holds configuration for all emitters.
type EmitterConfig struct {
	Stdout        StdoutEmitterConfig        `koanf:"stdout"`
	File          FileEmitterConfig          `koanf:"file"`
	Elasticsearch ElasticsearchEmitterConfig `koanf:"elasticsearch"`
	Loki          LokiEmitterConfig          `koanf:"loki"`
	VictoriaLogs  VictoriaLogsEmitterConfig  `koanf:"victorialogs"`
}

// StdoutEmitterConfig configures the stdout emitter.
type StdoutEmitterConfig struct {
	Enabled bool   `koanf:"enabled"`
	Format  string `koanf:"format"` // "json" or "text"
}

// FileEmitterConfig configures the file emitter.
type FileEmitterConfig struct {
	Enabled    bool   `koanf:"enabled"`
	Path       string `koanf:"path"`
	MaxSizeMB  int    `koanf:"maxsizemb" yaml:"max_size_mb" json:"max_size_mb"`
	MaxBackups int    `koanf:"maxbackups" yaml:"max_backups" json:"max_backups"`
	MaxAgeDays int    `koanf:"maxagedays" yaml:"max_age_days" json:"max_age_days"`
	Compress   bool   `koanf:"compress"`
}

// ElasticsearchEmitterConfig configures the Elasticsearch emitter.
type ElasticsearchEmitterConfig struct {
	Enabled       bool          `koanf:"enabled"`
	Addresses     []string      `koanf:"addresses"`
	Index         string        `koanf:"index"`
	Username      string        `koanf:"username"`
	Password      string        `koanf:"password"`
	BatchSize     int           `koanf:"batchsize" yaml:"batch_size" json:"batch_size"`
	FlushInterval time.Duration `koanf:"flushinterval" yaml:"flush_interval" json:"flush_interval"`
}

// LokiEmitterConfig configures the Loki emitter.
type LokiEmitterConfig struct {
	Enabled       bool              `koanf:"enabled"`
	URL           string            `koanf:"url"`
	TenantID      string            `koanf:"tenantid" yaml:"tenant_id" json:"tenant_id"`
	Labels        map[string]string `koanf:"labels"`
	BatchSize     int               `koanf:"batchsize" yaml:"batch_size" json:"batch_size"`
	FlushInterval time.Duration     `koanf:"flushinterval" yaml:"flush_interval" json:"flush_interval"`
}

// VictoriaLogsEmitterConfig configures the VictoriaLogs emitter.
type VictoriaLogsEmitterConfig struct {
	Enabled       bool          `koanf:"enabled"`
	URL           string        `koanf:"url"`
	BatchSize     int           `koanf:"batchsize" yaml:"batch_size" json:"batch_size"`
	FlushInterval time.Duration `koanf:"flushinterval" yaml:"flush_interval" json:"flush_interval"`
}

// defaults returns the default configuration values.
func defaults() Config {
	return Config{
		LogLevel: "info",
		Pipeline: PipelineConfig{
			BufferSize:       1000,
			ShutdownTimeout:  30 * time.Second,
			DropOnFullBuffer: false,
		},
		Ingestors: IngestorConfig{
			File: FileIngestorConfig{
				Enabled: false,
				Processor: ProcessorConfig{
					Parser: ParserConfig{
						Enabled:        true,
						JSONAutoDetect: true,
					},
					Enricher: EnricherConfig{
						Enabled:      true,
						AddHostname:  true,
						AddTimestamp: true,
					},
				},
			},
			Syslog: SyslogIngestorConfig{
				Enabled:  false,
				Protocol: "udp",
				Address:  ":514",
				Processor: ProcessorConfig{
					Parser:   ParserConfig{Enabled: true},
					Enricher: EnricherConfig{Enabled: true},
				},
			},
			Journal: JournalIngestorConfig{
				Enabled: false,
				Processor: ProcessorConfig{
					Parser:   ParserConfig{Enabled: true},
					Enricher: EnricherConfig{Enabled: true},
				},
			},
			Stdin: StdinIngestorConfig{
				Enabled: false,
				Processor: ProcessorConfig{
					Parser: ParserConfig{
						Enabled:        true,
						JSONAutoDetect: true,
					},
					Enricher: EnricherConfig{Enabled: true},
				},
			},
		},
		Emitters: EmitterConfig{
			Stdout: StdoutEmitterConfig{
				Enabled: true,
				Format:  "json",
			},
			File: FileEmitterConfig{
				Enabled:    false,
				MaxSizeMB:  100,
				MaxBackups: 3,
				MaxAgeDays: 7,
				Compress:   true,
			},
			Elasticsearch: ElasticsearchEmitterConfig{
				Enabled:       false,
				BatchSize:     100,
				FlushInterval: 5 * time.Second,
			},
			Loki: LokiEmitterConfig{
				Enabled:       false,
				BatchSize:     100,
				FlushInterval: 1 * time.Second,
			},
			VictoriaLogs: VictoriaLogsEmitterConfig{
				Enabled:       false,
				BatchSize:     100,
				FlushInterval: 1 * time.Second,
			},
		},
	}
}

// Load reads configuration from all sources with proper override order.
// Order: defaults -> config file -> environment variables.
func Load(configPath string) (*Config, error) {
	opts := []configloader.Option[Config]{
		configloader.WithDefaults[Config](defaults()),
	}

	// Add file source if path provided or if default config exists
	if configPath != "" {
		opts = append(opts, configloader.WithFile[Config](configPath))
	} else {
		// Try default config locations
		for _, path := range []string{"./config.yaml", "/etc/log-collector/config.yaml"} {
			if _, err := os.Stat(path); err == nil {
				opts = append(opts, configloader.WithFile[Config](path))
				break
			}
		}
	}

	// Add environment variable support
	opts = append(opts, configloader.WithEnv[Config]("LOG_COLLECTOR_"))

	// Load configuration
	loader := configloader.NewConfigLoader[Config](opts...)
	cfg, err := loader.Load()
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
