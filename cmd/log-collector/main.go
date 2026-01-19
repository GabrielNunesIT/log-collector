package main

import (
	"os"

	"github.com/GabrielNunesIT/log-collector/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		os.Exit(1)
	}
}
