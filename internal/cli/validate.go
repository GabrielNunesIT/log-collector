package cli

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/GabrielNunesIT/go-libs/logger"
	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/pipeline"
)

// NewValidateCmd creates the validate command.
func NewValidateCmd(cfgFile *string) *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate the configuration file",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(*cfgFile)
			if err != nil {
				return fmt.Errorf("configuration error: %w", err)
			}

			// Create a silent logger for validation (discards output)
			log := logger.NewConsoleLogger(io.Discard)

			p, err := pipeline.New(cfg, log)
			if err != nil {
				return fmt.Errorf("pipeline configuration error: %w", err)
			}

			fmt.Printf("Configuration valid:\n")
			fmt.Printf("  Ingestors: %d enabled\n", p.IngestorCount())
			fmt.Printf("  Emitters:  %d enabled\n", p.EmitterCount())
			return nil
		},
	}
}
