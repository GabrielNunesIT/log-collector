package cli

import (
	"fmt"

	"github.com/spf13/cobra"

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

			p, err := pipeline.New(cfg)
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
