package main

import (
	"context"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/bqs"
)

func main() {
	projectID := os.Getenv("TEST_PROJECT_ID")
	if projectID == "" {
		panic("TEST_PROJECT_ID is not set")
	}
	datasetID := os.Getenv("TEST_DATASET_ID")
	if datasetID == "" {
		panic("TEST_DATASET_ID is not set")
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		panic(err)
	}

	dataSet := client.Dataset(datasetID)
	tableName := time.Now().Format("test_20060102_150405")
	table := dataSet.Table(tableName)

	if err := Insert(ctx, table); err != nil {
		panic(err)
	}
}

type Row map[string]any

func (r Row) Save() (row map[string]bigquery.Value, insertID string, err error) {
	row = make(map[string]bigquery.Value)
	for k, v := range r {
		row[k] = v
	}
	return row, "", nil
}

func Insert(ctx context.Context, table *bigquery.Table) error {
	// Row is a map[string]any, implemented as bigquery.ValueSaver
	rows := []Row{
		{
			"CreatedAt": time.Now(),
			"Name":      "Alice",
			"Preferences": map[string]any{
				"Color": "Red",
			},
		},
		{
			"CreatedAt": time.Now(),
			"Name":      "Bob",
			"Age":       30,
		},
	}

	var mergedSchema bigquery.Schema
	for _, row := range rows {
		// If you use bigquery.InferSchema, it will fail to infer the schema of nested struct.
		schema, err := bqs.Infer(row)
		if err != nil {
			return err
		}

		// Merge the schema of each row
		mergedSchema, err = bqs.Merge(mergedSchema, schema)
		if err != nil {
			return err
		}
	}

	// Create a new table with the schema that is combined from all rows
	newMeta := &bigquery.TableMetadata{Schema: mergedSchema}
	if err := table.Create(ctx, newMeta); err != nil {
		return err
	}

	// Insert all rows to the created table
	if err := table.Inserter().Put(ctx, rows); err != nil {
		return err
	}

	return nil
}
