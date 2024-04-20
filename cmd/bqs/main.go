package main

import (
	"io"
	"os"
	"path/filepath"

	"github.com/m-mizutani/goerr"
	"github.com/urfave/cli/v2"
)

func main() {
	var (
		logLevel  string
		logOutput string
	)

	app := cli.App{
		Name: "bqs",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "log-level",
				Category:    "Log",
				Aliases:     []string{"l"},
				Usage:       "Log level (debug, info, warn, error)",
				Value:       "info",
				Destination: &logLevel,
			},
			&cli.StringFlag{
				Name:        "log-output",
				Category:    "Log",
				Aliases:     []string{"L"},
				Usage:       "Log output destination, stdout('-') or file path",
				Value:       "-",
				Destination: &logOutput,
			},
		},

		Before: func(c *cli.Context) error {
			var logWriter io.Writer
			switch logOutput {
			case "-", "stdout":
				logWriter = os.Stdout
			default:
				file, err := os.Create(filepath.Clean(logOutput))
				if err != nil {
					return goerr.Wrap(err, "Failed to open log file")
				}
				logWriter = file
			}

			return configureLogger(logLevel, logWriter)
		},

		Commands: []*cli.Command{
			inferCommand(),
		},
	}

	if err := app.Run(os.Args); err != nil {
		logger.Error("Failed", "error", err)
	}
}
