package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/bqs"
	"github.com/m-mizutani/goerr"
	"github.com/urfave/cli/v2"
)

func inferCommand() *cli.Command {
	var (
		output string
	)
	return &cli.Command{
		Name:        "infer",
		UsageText:   "bqs infer [command options] [json files...]",
		Description: "Infer schema from JSON data and output as BigQuery schema file. If no file is specified, read from stdin.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				Usage:       "Output schema file path",
				Value:       "-",
				Destination: &output,
			},
		},
		Action: func(c *cli.Context) error {
			var w io.Writer
			if output == "-" {
				w = os.Stdout
			} else {
				file, err := os.Create(filepath.Clean(output))
				if err != nil {
					return goerr.Wrap(err, "Failed to create schema file").With("path", output)
				}
				defer file.Close()
				w = file
			}

			type reader struct {
				r    io.Reader
				name string
			}

			var readers []*reader
			if c.Args().Len() == 0 {
				readers = append(readers, &reader{
					r:    os.Stdin,
					name: "(stdin)",
				})
			} else {
				for _, path := range c.Args().Slice() {
					file, err := os.Open(filepath.Clean(path))
					if err != nil {
						return goerr.Wrap(err, "Failed to open file").With("path", path)
					}
					defer file.Close()
					readers = append(readers, &reader{
						r:    file,
						name: path,
					})
				}
			}

			var schema bigquery.Schema
			for _, reader := range readers {
				logger.Debug("infer schema", "input", reader.name)

				decoder := json.NewDecoder(reader.r)
				for i := 0; ; i++ {
					var data any
					if err := decoder.Decode(&data); err != nil {
						if err == io.EOF {
							break
						}
						return goerr.Wrap(err, "Failed to decode JSON data").With("input", reader.name)
					}

					inferred, err := bqs.Infer(data)
					if err != nil {
						return goerr.Wrap(err, "Failed to infer schema").With("data", data).With("input", reader.name).With("line", i+1)
					}

					merged, err := bqs.Merge(schema, inferred)
					if err != nil {
						return goerr.Wrap(err, "Failed to merge schema").With("input", reader.name).With("line", i+1)
					}

					schema = merged
				}
			}

			raw, err := schema.ToJSONFields()
			if err != nil {
				return goerr.Wrap(err, "Failed to convert schema to JSON")
			}
			if _, err := w.Write(raw); err != nil {
				return goerr.Wrap(err, "Failed to write schema").With("output", output)
			}

			return nil
		},
	}
}
