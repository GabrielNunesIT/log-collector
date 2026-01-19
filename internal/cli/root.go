package cli

import (
	"github.com/spf13/cobra"
)

// Execute builds and runs the CLI.
func Execute() error {
	var (
		cfgFile  string
		logLevel string
	)

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

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default: ./config.yaml)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "log level (debug, info, warn, error)")

	rootCmd.AddCommand(
		NewRunCmd(&cfgFile, &logLevel),
		NewValidateCmd(&cfgFile),
		NewVersionCmd(),
	)

	return rootCmd.Execute()
}
