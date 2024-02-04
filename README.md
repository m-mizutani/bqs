# bqs

Utility for inferring, merging and comparing BigQuery schema in Go.

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

## License

Apache License 2.0

