package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	Version   = "dev"
	GitCommit = "unknown"
	BuildDate = "unknown"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version of kafka-replay",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("kafka-replay %s (commit: %s, built: %s)\n", Version, GitCommit, BuildDate)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
