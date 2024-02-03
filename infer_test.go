package bqs_test

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/bqs"
	"github.com/m-mizutani/gt"
)

func matchSchema(t testing.TB, schemas bigquery.Schema, expect bigquery.Schema) {
	t.Helper()
	gt.A(t, schemas).Length(len(expect))
	for _, e := range expect {
		gt.A(t, schemas).MatchThen(func(s *bigquery.FieldSchema) bool {
			return s.Name == e.Name
		}, func(t testing.TB, s *bigquery.FieldSchema) {
			gt.Equal(t, s.Type, e.Type)
			if e.Schema != nil {
				matchSchema(t, s.Schema, e.Schema)
			}
		})
	}
}

func TestMatchSchema(t *testing.T) {
	testCases := map[string]struct {
		Schemas bigquery.Schema
		Expect  bigquery.Schema
		Fail    bool
	}{
		"match": {
			Schemas: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
				{
					Name: "key2",
					Type: bigquery.NumericFieldType,
				},
			},
			Expect: bigquery.Schema{
				{
					Name: "key2",
					Type: bigquery.NumericFieldType,
				},
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
			},
		},
		"mismatch": {
			Schemas: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
				{
					Name: "key2",
					Type: bigquery.NumericFieldType,
				},
			},
			Expect: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
			},
			Fail: true,
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
			Fail: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t2 := &testing.T{}
			matchSchema(t2, tc.Schemas, tc.Expect)
			gt.Equal(t, tc.Fail, t2.Failed())
		})
	}
}

func TestBasicFields(t *testing.T) {
	var row struct {
		Str     string
		Int     int
		Int16   int16
		Int32   int32
		Int64   int64
		Uint    uint
		Uint8   uint8
		Uint16  uint16
		Uint32  uint32
		Uint64  uint64
		Float32 float32
		Float64 float64
		Bool    bool
	}

	schemas := gt.R1(bqs.Infer(row)).NoError(t)
	gt.A(t, schemas).Length(13).Have(&bigquery.FieldSchema{
		Name: "Str", Type: bigquery.StringFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Int", Type: bigquery.NumericFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Int16", Type: bigquery.NumericFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Int32", Type: bigquery.NumericFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Int64", Type: bigquery.NumericFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Uint", Type: bigquery.NumericFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Uint8", Type: bigquery.NumericFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Uint16", Type: bigquery.NumericFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Uint32", Type: bigquery.NumericFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Uint64", Type: bigquery.NumericFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Float32", Type: bigquery.FloatFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Float64", Type: bigquery.FloatFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Bool", Type: bigquery.BooleanFieldType,
	})
}

func TestNestedStruct(t *testing.T) {
	var row struct {
		Str   string
		Nest1 struct {
			Int   int
			Nest2 struct {
				Bool bool
			}
		}
	}

	schemas := gt.R1(bqs.Infer(row)).NoError(t)
	gt.A(t, schemas).Length(2).At(0, func(t testing.TB, v *bigquery.FieldSchema) {
		gt.Equal(t, v.Name, "Str")
		gt.Equal(t, v.Type, bigquery.StringFieldType)
	}).At(1, func(t testing.TB, v *bigquery.FieldSchema) {
		gt.Equal(t, v.Name, "Nest1")
		gt.Equal(t, v.Type, bigquery.RecordFieldType)
		gt.A(t, v.Schema).Length(2).At(0, func(t testing.TB, v *bigquery.FieldSchema) {
			gt.Equal(t, v.Name, "Int")
			gt.Equal(t, v.Type, bigquery.NumericFieldType)
		}).At(1, func(t testing.TB, v *bigquery.FieldSchema) {
			gt.Equal(t, v.Name, "Nest2")
			gt.Equal(t, v.Type, bigquery.RecordFieldType)
			gt.A(t, v.Schema).Length(1).At(0, func(t testing.TB, v *bigquery.FieldSchema) {
				gt.Equal(t, v.Name, "Bool")
				gt.Equal(t, v.Type, bigquery.BooleanFieldType)
			})
		})
	})
}

func TestNestedPointerStruct(t *testing.T) {
	type nest2 struct {
		Bool bool
	}
	type nest1 struct {
		Int   int
		Nest2 *nest2
	}
	row := nest1{
		Int: 1,
		Nest2: &nest2{
			Bool: true,
		},
	}

	schemas := gt.R1(bqs.Infer(row)).NoError(t)
	matchSchema(t, schemas, bigquery.Schema{
		{
			Name: "Int",
			Type: bigquery.NumericFieldType,
		},
		{
			Name: "Nest2",
			Type: bigquery.RecordFieldType,
			Schema: bigquery.Schema{
				{
					Name: "Bool",
					Type: bigquery.BooleanFieldType,
				},
			},
		},
	})
}

func TestMap(t *testing.T) {
	testCases := map[string]struct {
		Input  any
		Expect bigquery.Schema
	}{
		"string map": {
			Input: map[string]string{
				"key1": "value1",
			},
			Expect: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
			},
		},
		"int map": {
			Input: map[string]int{
				"key1": 1,
			},
			Expect: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.NumericFieldType,
				},
			},
		},
		"nested map": {
			Input: map[string]map[string]string{
				"key1": {
					"key2": "value2",
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
		},
		"nested map with pointer": {
			Input: map[string]*map[string]string{
				"key1": {
					"key2": "value2",
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
		},
		"nested map with struct": {
			Input: map[string]struct {
				Key2 string
			}{
				"key1": {
					Key2: "value2",
				},
			},
			Expect: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.RecordFieldType,
					Schema: bigquery.Schema{
						{
							Name: "Key2",
							Type: bigquery.StringFieldType,
						},
					},
				},
			},
		},
		"nested map with pointer struct": {
			Input: map[string]*struct {
				Key2 string
			}{
				"key1": {
					Key2: "value2",
				},
			},
			Expect: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.RecordFieldType,
					Schema: bigquery.Schema{
						{
							Name: "Key2",
							Type: bigquery.StringFieldType,
						},
					},
				},
			},
		},
		"nested map with map": {
			Input: map[string]map[string]map[string]string{
				"key1": {
					"key2": {
						"key3": "value3",
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
							Type: bigquery.RecordFieldType,
							Schema: bigquery.Schema{
								{
									Name: "key3",
									Type: bigquery.StringFieldType,
								},
							},
						},
					},
				},
			},
		},
		"any map": {
			Input: map[string]interface{}{
				"key1": "value1",
				"key2": 2,
			},
			Expect: bigquery.Schema{
				{
					Name: "key1",
					Type: bigquery.StringFieldType,
				},
				{
					Name: "key2",
					Type: bigquery.NumericFieldType,
				},
			},
		},
		"integer key map": {
			Input: map[int]string{
				1: "value1",
			},
			Expect: nil,
		},
		"struct key map": {
			Input: map[struct {
				Key string
			}]string{
				{Key: "key1"}: "value1",
			},
			Expect: nil,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			schemas, err := bqs.Infer(tc.Input)
			if tc.Expect == nil {
				gt.Error(t, err)
			} else {
				gt.NoError(t, err)
				matchSchema(t, schemas, tc.Expect)
			}
		})
	}
}
