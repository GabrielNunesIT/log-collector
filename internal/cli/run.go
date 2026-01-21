package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/GabrielNunesIT/go-libs/logger"
	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/pipeline"
)

// NewRunCmd creates the run command.
func NewRunCmd(cfgFile, logLevel *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Start the log collector pipeline",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPipeline(cmd, cfgFile, logLevel)
		},
	}

	// Ingestor flags
	cmd.Flags().Bool("stdin", false, "enable stdin ingestor")
	cmd.Flags().StringSlice("file", nil, "file paths to tail (enables file ingestor)")
	cmd.Flags().String("syslog-address", "", "syslog listen address (enables syslog ingestor)")
	cmd.Flags().Bool("journal", false, "enable systemd journal ingestor")

	// Emitter flags
	cmd.Flags().Bool("stdout", false, "enable stdout emitter")
	cmd.Flags().String("stdout-format", "json", "stdout output format (json, text)")

	// Hot-reload flag
	cmd.Flags().Bool("hot-reload", true, "enable hot-reload of config file")

	return cmd
}

func runPipeline(cmd *cobra.Command, cfgFile, logLevel *string) error {
	log := SetupLogging(*logLevel)

	cfg, err := config.Load(*cfgFile)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	applyCLIOverrides(cmd, cfg)

	p, err := pipeline.New(cfg, log)
	if err != nil {
		return fmt.Errorf("creating pipeline: %w", err)
	}

	log.Infof("starting log collector: ingestors=%d, emitters=%d",
		p.IngestorCount(), p.EmitterCount())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	hotReloadEnabled, _ := cmd.Flags().GetBool("hot-reload")
	if *cfgFile != "" && hotReloadEnabled {
		startConfigWatcher(ctx, cmd, cfgFile, p, log)
	}

	go handleSignals(ctx, cancel, sigChan, cmd, cfgFile, p, log)

	if err := p.Run(ctx); err != nil && err != context.Canceled {
		return fmt.Errorf("pipeline error: %w", err)
	}

	log.Info("log collector stopped")
	return nil
}

func startConfigWatcher(ctx context.Context, cmd *cobra.Command, cfgFile *string, p *pipeline.Pipeline, log logger.ILogger) {
	watcher := config.NewConfigWatcher(*cfgFile, log)
	if err := watcher.Start(ctx); err != nil {
		log.Warningf("failed to start config watcher: %v", err)
		return
	}

	log.Infof("hot-reload enabled: config=%s", *cfgFile)

	go func() {
		for {
			select {
			case newCfg := <-watcher.Changes():
				applyCLIOverrides(cmd, newCfg)
				if err := p.Reconfigure(newCfg); err != nil {
					log.Errorf("reconfigure failed: %v", err)
				}
			case err := <-watcher.Errors():
				log.Errorf("config watcher error: %v", err)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func handleSignals(ctx context.Context, cancel context.CancelFunc, sigChan <-chan os.Signal, cmd *cobra.Command, cfgFile *string, p *pipeline.Pipeline, log logger.ILogger) {
	for {
		select {
		case sig := <-sigChan:
			switch sig {
			case syscall.SIGHUP:
				log.Info("received SIGHUP, reloading config")
				newCfg, err := config.Load(*cfgFile)
				if err != nil {
					log.Errorf("failed to reload config: %v", err)
					continue
				}
				applyCLIOverrides(cmd, newCfg)
				if err := p.Reconfigure(newCfg); err != nil {
					log.Errorf("reconfigure failed: %v", err)
				}
			case syscall.SIGINT, syscall.SIGTERM:
				log.Infof("received shutdown signal: %v", sig)
				cancel()
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func applyCLIOverrides(cmd *cobra.Command, cfg *config.Config) {
	if v, _ := cmd.Flags().GetBool("stdin"); v {
		cfg.Ingestors.Stdin.Enabled = true
	}
	if v, _ := cmd.Flags().GetBool("journal"); v {
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
	if v, _ := cmd.Flags().GetBool("stdout"); v {
		cfg.Emitters.Stdout.Enabled = true
	}
	if format, _ := cmd.Flags().GetString("stdout-format"); format != "" {
		cfg.Emitters.Stdout.Format = format
	}
}
