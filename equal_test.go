package bqs_test

import (
	"context"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/bqs"
	"github.com/m-mizutani/gt"
)

func TestEqual(t *testing.T) {
	testCases := map[string]struct {
		Schemas bigquery.Schema
		Expect  bigquery.Schema
		Match   bool
	}{
		"match": {
			Schemas: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
				{
					Name: "key2",
					Type: bigquery.IntegerFieldType,
				},
			},
			Expect: bigquery.Schema{
				{
					Name: "key2",
					Type: bigquery.IntegerFieldType,
				},
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
			},
			Match: true,
		},
		"mismatch": {
			Schemas: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
				{
					Name: "key2",
					Type: bigquery.IntegerFieldType,
				},
			},
			Expect: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
			},
			Match: false,
		},
		"match nested": {
			Schemas: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.RecordFieldType,
					Schema: bigquery.Schema{
						{
							Name: "key2",
							Type: bigquery.StringFieldType,
						},
					},
				},
			},
			Expect: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.RecordFieldType,
					Schema: bigquery.Schema{
						{
							Name: "key2",
							Type: bigquery.StringFieldType,
						},
					},
				},
			},
			Match: true,
		},
		"mismatch nested": {
			Schemas: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.RecordFieldType,
					Schema: bigquery.Schema{
						{
							Name: "key2",
							Type: bigquery.StringFieldType,
						},
					},
				},
			},
			Expect: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.RecordFieldType,
					Schema: bigquery.Schema{
						{
							Name: "key3",
							Type: bigquery.StringFieldType,
						},
					},
				},
			},
			Match: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			gt.Equal(t, tc.Match, bqs.Equal(tc.Schemas, tc.Expect))
		})
	}
}

func TestEqualWithBigQuery(t *testing.T) {
	projectID := os.Getenv("TEST_PROJECT_ID")
	if projectID == "" {
		t.Skip("TEST_PROJECT_ID is not set")
	}
	datasetID := os.Getenv("TEST_DATASET_ID")
	if datasetID == "" {
		t.Skip("TEST_DATASET_ID is not set")
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		panic(err)
	}

	dataSet := client.Dataset(datasetID)
	tableName := time.Now().Format("test_20060102_150405")
	table := dataSet.Table(tableName)

	type SampleData struct {
		Key1 string
		Key2 int
	}

	d1 := SampleData{
		Key1: "value1",
		Key2: 2,
	}

	schema := gt.R1(bqs.Infer(d1)).NoError(t)
	tm := &bigquery.TableMetadata{
		Schema: schema,
	}
	gt.NoError(t, table.Create(ctx, tm))

	created := gt.R1(table.Metadata(ctx)).NoError(t)
	gt.True(t, bqs.Equal(created.Schema, schema))
}
