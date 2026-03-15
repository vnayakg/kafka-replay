package main

import (
	"os"

	"github.com/vnayakg/kafka-replay/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
