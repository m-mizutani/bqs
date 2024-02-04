package bqs

import "cloud.google.com/go/bigquery"

// Equal compares two bigquery.Schema and returns true if they are equal.
// It returns false if the length of the schemas are different or the fields are different.
func Equal(a, b bigquery.Schema) bool {
	if len(a) != len(b) {
		return false
	}

	for _, p := range a {
		if !contain(b, p) {
			return false
		}
	}

	return true
}

func equalFieldSchema(a, b *bigquery.FieldSchema) bool {
	// TODO: check PolicyTags
	// TODO: check RangeElementType
	return a.Name == b.Name &&
		a.Type == b.Type &&
		a.Description == b.Description &&
		a.Required == b.Required &&
		a.Repeated == b.Repeated &&
		a.MaxLength == b.MaxLength &&
		a.Precision == b.Precision &&
		a.Scale == b.Scale &&
		a.DefaultValueExpression == b.DefaultValueExpression &&
		a.Collation == b.Collation &&
		Equal(a.Schema, b.Schema)
}

func contain(s bigquery.Schema, p *bigquery.FieldSchema) bool {
	for _, q := range s {
		if equalFieldSchema(q, p) {
			return true
		}
	}

	return false
}
