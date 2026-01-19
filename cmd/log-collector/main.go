package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/user/log-collector/internal/config"
	"github.com/user/log-collector/internal/pipeline"
)

var (
	cfgFile  string
	logLevel string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "log-collector",
		Short: "A high-performance log collector with pluggable ingestors and emitters",
		Long: `log-collector is a goroutine-based log collection agent that supports
multiple input sources (file, syslog, journal, stdin) and output destinations
(stdout, file, elasticsearch, loki, victorialogs).

Each ingestor has its dedicated processor chain. All processed logs are
fan-out to every configured emitter.

Hot-reload: When a config file is specified, changes are automatically applied
without requiring a restart.`,
	}

	// Persistent flags
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default: ./config.yaml)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "log level (debug, info, warn, error)")

	// Run command
	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Start the log collector pipeline",
		RunE:  runPipeline,
	}

	// Add ingestor flags
	runCmd.Flags().Bool("stdin", false, "enable stdin ingestor")
	runCmd.Flags().StringSlice("file", nil, "file paths to tail (enables file ingestor)")
	runCmd.Flags().String("syslog-address", "", "syslog listen address (enables syslog ingestor)")
	runCmd.Flags().Bool("journal", false, "enable systemd journal ingestor")

	// Add emitter flags
	runCmd.Flags().Bool("stdout", false, "enable stdout emitter")
	runCmd.Flags().String("stdout-format", "json", "stdout output format (json, text)")

	// Hot-reload flag
	runCmd.Flags().Bool("hot-reload", true, "enable hot-reload of config file")

	// Validate command
	validateCmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate the configuration file",
		RunE:  validateConfig,
	}

	// Version command
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("log-collector v0.2.0")
		},
	}

	rootCmd.AddCommand(runCmd, validateCmd, versionCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// runPipeline starts the log collection pipeline.
func runPipeline(cmd *cobra.Command, args []string) error {
	// Setup logging
	setupLogging()

	// Load configuration
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// Apply CLI flag overrides
	applyCLIOverrides(cmd, cfg)

	// Create pipeline
	p, err := pipeline.New(cfg)
	if err != nil {
		return fmt.Errorf("creating pipeline: %w", err)
	}

	slog.Info("starting log collector",
		"ingestors", p.IngestorCount(),
		"emitters", p.EmitterCount(),
	)

	// Setup context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// Start hot-reload watcher if config file is specified
	hotReloadEnabled, _ := cmd.Flags().GetBool("hot-reload")
	if cfgFile != "" && hotReloadEnabled {
		watcher := config.NewConfigWatcher(cfgFile)
		if err := watcher.Start(ctx); err != nil {
			slog.Warn("failed to start config watcher", "error", err)
		} else {
			slog.Info("hot-reload enabled", "config", cfgFile)

			// Handle config changes
			go func() {
				for {
					select {
					case newCfg := <-watcher.Changes():
						// Apply CLI overrides to new config too
						applyCLIOverrides(cmd, newCfg)
						if err := p.Reconfigure(newCfg); err != nil {
							slog.Error("reconfigure failed", "error", err)
						}
					case err := <-watcher.Errors():
						slog.Error("config watcher error", "error", err)
					case <-ctx.Done():
						return
					}
				}
			}()
		}
	}

	// Handle signals
	go func() {
		for {
			sig := <-sigChan
			switch sig {
			case syscall.SIGHUP:
				// SIGHUP triggers config reload
				slog.Info("received SIGHUP, reloading config")
				newCfg, err := config.Load(cfgFile)
				if err != nil {
					slog.Error("failed to reload config", "error", err)
					continue
				}
				applyCLIOverrides(cmd, newCfg)
				if err := p.Reconfigure(newCfg); err != nil {
					slog.Error("reconfigure failed", "error", err)
				}
			case syscall.SIGINT, syscall.SIGTERM:
				slog.Info("received shutdown signal", "signal", sig)
				cancel()
				return
			}
		}
	}()

	// Run pipeline
	if err := p.Run(ctx); err != nil && err != context.Canceled {
		return fmt.Errorf("pipeline error: %w", err)
	}

	slog.Info("log collector stopped")
	return nil
}

// applyCLIOverrides applies command-line flag overrides to config.
func applyCLIOverrides(cmd *cobra.Command, cfg *config.Config) {
	if stdinEnabled, _ := cmd.Flags().GetBool("stdin"); stdinEnabled {
		cfg.Ingestors.Stdin.Enabled = true
	}

	if journalEnabled, _ := cmd.Flags().GetBool("journal"); journalEnabled {
		cfg.Ingestors.Journal.Enabled = true
	}

	if files, _ := cmd.Flags().GetStringSlice("file"); len(files) > 0 {
		cfg.Ingestors.File.Enabled = true
		cfg.Ingestors.File.Paths = files
	}

	if addr, _ := cmd.Flags().GetString("syslog-address"); addr != "" {
		cfg.Ingestors.Syslog.Enabled = true
		cfg.Ingestors.Syslog.Address = addr
	}

	if stdoutEnabled, _ := cmd.Flags().GetBool("stdout"); stdoutEnabled {
		cfg.Emitters.Stdout.Enabled = true
	}

	if format, _ := cmd.Flags().GetString("stdout-format"); format != "" {
		cfg.Emitters.Stdout.Format = format
	}
}

// validateConfig validates the configuration without running.
func validateConfig(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}

	// Try to create pipeline (validates all components)
	p, err := pipeline.New(cfg)
	if err != nil {
		return fmt.Errorf("pipeline configuration error: %w", err)
	}

	fmt.Printf("Configuration valid:\n")
	fmt.Printf("  Ingestors: %d enabled\n", p.IngestorCount())
	fmt.Printf("  Emitters:  %d enabled\n", p.EmitterCount())
	return nil
}

// setupLogging configures structured logging.
func setupLogging() {
	var level slog.Level
	switch logLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})
	slog.SetDefault(slog.New(handler))
}
