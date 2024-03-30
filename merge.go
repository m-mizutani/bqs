package bqs

import (
	"fmt"

	"cloud.google.com/go/bigquery"
)

// Merge merges two bigquery.Schema and returns a new bigquery.Schema.
// It returns an error if the schemas are not compatible.
// If the field Name is not found in the old schema, it will be added to the result.
// If the field Name is found in the old schema, it will be replaced with the new field.
// If the field Type, Repeated, Required is different, it will return an error.
// In other cases, old field will be overwritten by new field.
func Merge(old, new bigquery.Schema) (bigquery.Schema, error) {
	return merge("", old, new)
}

func merge(path string, old, new bigquery.Schema) (bigquery.Schema, error) {
	var result bigquery.Schema

	oldFields := make(map[string]*bigquery.FieldSchema)
	for _, p := range old {
		oldFields[p.Name] = p
	}

	for _, p := range new {
		exist := lookupField(old, p.Name)
		if exist == nil {
			result = append(result, p)
			continue
		}
		delete(oldFields, p.Name)

		merged, err := mergeField(path, exist, p)
		if err != nil {
			return nil, err
		}

		if merged != nil {
			result = append(result, merged)
		}
	}

	for _, p := range oldFields {
		result = append(result, p)
	}

	return result, nil
}

func lookupField(s bigquery.Schema, name string) *bigquery.FieldSchema {
	for i, p := range s {
		if p.Name == name {
			return s[i]
		}
	}
	return nil
}

func boolToStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func mergeField(path string, old, new *bigquery.FieldSchema) (*bigquery.FieldSchema, error) {
	merged := *new
	if old.Type != new.Type {
		return nil, fmt.Errorf("type conflict: field='%s%s' (old=%s, new=%s): %w", path, old.Name, old.Type, new.Type, ErrConflictField)
	}

	if old.Repeated != new.Repeated {
		return nil, fmt.Errorf("repeated conflict: field='%s%s' (old=%s, new=%s): %w", path, old.Name, boolToStr(old.Repeated), boolToStr(new.Repeated), ErrConflictField)
	}

	if old.Required != new.Required {
		return nil, fmt.Errorf("required conflict: field='%s%s' (old=%s, new=%s): %w", path, old.Name, boolToStr(old.Required), boolToStr(new.Required), ErrConflictField)
	}

	if old.Schema == nil {
		merged.Schema = new.Schema
	} else {
		if new.Schema != nil {
			schema, err := merge(path+new.Name+".", old.Schema, new.Schema)
			if err != nil {
				return nil, err
			}
			merged.Schema = schema
		} else {
			merged.Schema = old.Schema
		}
	}

	return &merged, nil
}
