# bqs
[![Go Reference](https://pkg.go.dev/badge/github.com/m-mizutani/bqs.svg)](https://pkg.go.dev/github.com/m-mizutani/bqs) [![test](https://github.com/m-mizutani/bqs/actions/workflows/test.yml/badge.svg)](https://github.com/m-mizutani/bqs/actions/workflows/test.yml) [![gosec](https://github.com/m-mizutani/bqs/actions/workflows/gosec.yml/badge.svg)](https://github.com/m-mizutani/bqs/actions/workflows/gosec.yml) [![trivy](https://github.com/m-mizutani/bqs/actions/workflows/trivy.yml/badge.svg)](https://github.com/m-mizutani/bqs/actions/workflows/trivy.yml) [![lint](https://github.com/m-mizutani/bqs/actions/workflows/lint.yml/badge.svg)](https://github.com/m-mizutani/bqs/actions/workflows/lint.yml)

Utility for inferring, merging and comparing BigQuery schema in Go. BigQuery provides a feature to infer schema, such as [bigquery.InferSchema](https://pkg.go.dev/cloud.google.com/go/bigquery#InferSchema) and [schema auto detection](https://cloud.google.com/bigquery/docs/schema-detect). However, `bigquery.InferSchema` does not support nested struct and map. schema auto detection is not available in the Go client library. This library provides a way to infer BigQuery schema from nested Go struct and map.

## Features

- [x] Infer BigQuery schema from **nested** Go struct and map
- [x] Merge BigQuery schema
- [x] Compare BigQuery schema

## Example

```go
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
```

This will create a table with the following schema and insert the rows.

| CreatedAt                  | Name  | Age | Preferences.Color |
|----------------------------|-------|-------|-------------------|
| 2024-02-04 03:29:55.504942 | Alice | *null*    | Red               |
| 2024-02-04 03:29:55.504943 | Bob   | 30  | *null*                 |

## CLI

### Install

```bash
go install github.com/m-mizutani/bqs/cmd/bqs@latest
```

### Example

```bash
$ cat test.jsonl
{"color":"blue", "number":5, "property":{"age": 18}}
{"color":"green", "number":1, "property":{"name":"Alice"}}
$ bqs infer test.jsonl
[
 {
  "name": "color",
  "type": "STRING"
 },
 {
  "name": "number",
  "type": "FLOAT"
 },
 {
  "fields": [
   {
    "name": "name",
    "type": "STRING"
   },
   {
    "name": "age",
    "type": "FLOAT"
   }
  ],
  "name": "property",
  "type": "RECORD"
 }
]
```

## License

Apache License 2.0

