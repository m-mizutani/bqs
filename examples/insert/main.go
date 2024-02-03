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
	if err := insertExample(ctx, dataSet, tableName); err != nil {
		panic(err)
	}
}

func insertExample(ctx context.Context, dataset *bigquery.Dataset, tableID string) error {
	type UserField struct {
		Name string
		Age  int
	}
	row := struct {
		LogID string
		User  *UserField
	}{
		LogID: time.Now().Format("log_20060102_150405"),
		User: &UserField{
			Name: "Alice",
			Age:  20,
		},
	}

	schema, err := bqs.Infer(row)
	if err != nil {
		return err
	}

	table := dataset.Table(tableID)
	tm := &bigquery.TableMetadata{
		Schema: schema,
	}
	if err := table.Create(ctx, tm); err != nil {
		return err
	}

	if err := table.Inserter().Put(ctx, row); err != nil {
		return err
	}

	return nil
}
