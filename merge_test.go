package bqs_test

import (
	"errors"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/bqs"
)

func TestMerge(t *testing.T) {
	testCases := map[string]struct {
		oldSchema      bigquery.Schema
		newSchema      bigquery.Schema
		expectedSchema bigquery.Schema
		expectedError  error
	}{
		"merge compatible schemas": {
			oldSchema: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
				{
					Name: "key2",
					Type: bigquery.IntegerFieldType,
				},
			},
			newSchema: bigquery.Schema{
				{
					Name: "key2",
					Type: bigquery.IntegerFieldType,
				},
				{
					Name: "key3",
					Type: bigquery.StringFieldType,
				},
			},
			expectedSchema: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
				{
					Name: "key2",
					Type: bigquery.IntegerFieldType,
				},
				{
					Name: "key3",
					Type: bigquery.StringFieldType,
				},
			},
			expectedError: nil,
		},
		"conflict type": {
			oldSchema: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
			},
			newSchema: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.IntegerFieldType,
				},
			},
			expectedSchema: nil,
			expectedError:  bqs.ErrConflictField,
		},
		"conflict repeated": {
			oldSchema: bigquery.Schema{
				{
					Name:     "key1",
					Type:     bigquery.StringFieldType,
					Repeated: true,
				},
			},
			newSchema: bigquery.Schema{
				{
					Name:     "key1",
					Type:     bigquery.StringFieldType,
					Repeated: false,
				},
			},
			expectedSchema: nil,
			expectedError:  bqs.ErrConflictField,
		},
		"conflict required": {
			oldSchema: bigquery.Schema{
				{
					Name:     "key1",
					Type:     bigquery.StringFieldType,
					Required: true,
				},
			},
			newSchema: bigquery.Schema{
				{
					Name:     "key1",
					Type:     bigquery.StringFieldType,
					Required: false,
				},
			},
			expectedSchema: nil,
			expectedError:  bqs.ErrConflictField,
		},
		"conflict nested schema": {
			oldSchema: bigquery.Schema{
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
			newSchema: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.RecordFieldType,
					Schema: bigquery.Schema{
						{
							Name: "key2",
							Type: bigquery.IntegerFieldType,
						},
					},
				},
			},
			expectedSchema: nil,
			expectedError:  bqs.ErrConflictField,
		},
		"merge nested schema": {
			oldSchema: bigquery.Schema{
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
			newSchema: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.RecordFieldType,
					Schema: bigquery.Schema{
						{
							Name: "key3",
							Type: bigquery.IntegerFieldType,
						},
					},
				},
			},
			expectedSchema: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.RecordFieldType,
					Schema: bigquery.Schema{
						{
							Name: "key2",
							Type: bigquery.StringFieldType,
						},
						{
							Name: "key3",
							Type: bigquery.IntegerFieldType,
						},
					},
				},
			},
			expectedError: nil,
		},
		"merge nested schema with conflict": {
			oldSchema: bigquery.Schema{
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
			newSchema: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.RecordFieldType,
					Schema: bigquery.Schema{
						{
							Name: "key2",
							Type: bigquery.IntegerFieldType,
						},
						{
							Name: "key3",
							Type: bigquery.IntegerFieldType,
						},
					},
				},
			},
			expectedSchema: nil,
			expectedError:  bqs.ErrConflictField,
		},
		"merge duplicated field name": {
			oldSchema: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
			},
			newSchema: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
			},
			expectedSchema: nil,
			expectedError:  bqs.ErrConflictField,
		},
		"merge case insensitive duplicated field name": {
			oldSchema: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
			},
			newSchema: bigquery.Schema{
				{
					Name: "KEY1",
					Type: bigquery.StringFieldType,
				},
			},
			expectedSchema: nil,
			expectedError:  bqs.ErrConflictField,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			mergedSchema, err := bqs.Merge(tc.oldSchema, tc.newSchema)
			if tc.expectedError != nil {
				if !errors.Is(err, tc.expectedError) {
					t.Errorf("unexpected error: got %v, want %v", err, tc.expectedError)
				}
			} else if !bqs.Equal(mergedSchema, tc.expectedSchema) {
				t.Errorf("unexpected merged schema: got %v, want %v", mergedSchema, tc.expectedSchema)
			}
		})
	}
}

func TestMergeConflictError(t *testing.T) {
	a := bigquery.Schema{
		{
			Name: "key1",
			Type: bigquery.RecordFieldType,
			Schema: bigquery.Schema{
				{
					Name: "key2",
					Type: bigquery.IntegerFieldType,
				},
				{
					Name: "key3",
					Type: bigquery.IntegerFieldType,
				},
			},
		},
	}
	b := bigquery.Schema{
		{
			Name: "key1",
			Type: bigquery.RecordFieldType,
			Schema: bigquery.Schema{
				{
					Name: "key2",
					Type: bigquery.IntegerFieldType,
				},
				{
					Name: "key3",
					Type: bigquery.StringFieldType,
				},
			},
		},
	}

	_, err := bqs.Merge(a, b)
	if !strings.Contains(err.Error(), "field='key1.key3'") {
		t.Errorf("expected to contain field path, but not: %s", err.Error())
	}
}
