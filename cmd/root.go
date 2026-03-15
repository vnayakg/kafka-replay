package cmd

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "kafka-replay",
	Short: "Replay Kafka messages from one topic to another",
	Long: `kafka-replay is a high-performance CLI tool for replaying Kafka messages
from a source topic (typically a Dead Letter Topic) to a target topic.

It supports partition-parallel consumption, batched production, filtering
by offset/timestamp ranges, checkpointing for resumability, and rate limiting.`,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func Execute() error {
	return rootCmd.Execute()
}
