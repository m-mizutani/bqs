package main

import (
	"context"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/bqs"
	"google.golang.org/api/googleapi"
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

	log1 := &LogData1{
		LogID: time.Now().Format("log_20060102_150405"),
		User: &UserField{
			Name: "Alice",
			Age:  20,
		},
	}

	if err := Insert(ctx, table, log1); err != nil {
		panic(err)
	}

	log2 := &LogData2{
		LogID:     time.Now().Format("log_20060102_150405"),
		Timestamp: time.Now(),
		User: &UserField{
			Name: "Bob",
			Age:  30,
		},
	}

	if err := Insert(ctx, table, log2); err != nil {
		panic(err)
	}
}

type LogData1 struct {
	LogID string
	User  *UserField
}

type LogData2 struct {
	LogID     string
	Timestamp time.Time
	User      *UserField
}

type UserField struct {
	Name string
	Age  int
}

func Insert(ctx context.Context, table *bigquery.Table, data any) error {
	schema, err := bqs.Infer(data)
	if err != nil {
		return err
	}

	if tm, err := table.Metadata(ctx); err != nil {
		if gerr, ok := err.(*googleapi.Error); !ok || gerr.Code != 404 {
			return err
		}

		newMeta := &bigquery.TableMetadata{
			Schema: schema,
		}
		if err := table.Create(ctx, newMeta); err != nil {
			return err
		}
	} else if !bqs.Equal(tm.Schema, schema) {

	}

	if err := table.Inserter().Put(ctx, data); err != nil {
		return err
	}

	return nil
}
