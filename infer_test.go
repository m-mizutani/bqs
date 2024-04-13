package bqs_test

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-mizutani/bqs"
	"github.com/m-mizutani/gt"
)

func TestBasicFields(t *testing.T) {
	var row struct {
		Str       string
		Int       int
		Int16     int16
		Int32     int32
		Int64     int64
		Uint      uint
		Uint8     uint8
		Uint16    uint16
		Uint32    uint32
		Uint64    uint64
		Float32   float32
		Float64   float64
		Bool      bool
		Timestamp time.Time
	}

	schemas := gt.R1(bqs.Infer(row)).NoError(t)
	gt.A(t, schemas).Length(14).Have(&bigquery.FieldSchema{
		Name: "Str", Type: bigquery.StringFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Int", Type: bigquery.IntegerFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Int16", Type: bigquery.IntegerFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Int32", Type: bigquery.IntegerFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Int64", Type: bigquery.IntegerFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Uint", Type: bigquery.IntegerFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Uint8", Type: bigquery.IntegerFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Uint16", Type: bigquery.IntegerFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Uint32", Type: bigquery.IntegerFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Uint64", Type: bigquery.IntegerFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Float32", Type: bigquery.FloatFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Float64", Type: bigquery.FloatFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Bool", Type: bigquery.BooleanFieldType,
	}).Have(&bigquery.FieldSchema{
		Name: "Timestamp", Type: bigquery.TimestampFieldType,
	})
}

func TestTime(t *testing.T) {
	t.Run("time.Time value", func(t *testing.T) {
		row := struct {
			Time time.Time
		}{
			Time: time.Now(),
		}
		schema := gt.R1(bqs.Infer(row)).NoError(t)
		gt.A(t, schema).Length(1).At(0, func(t testing.TB, v *bigquery.FieldSchema) {
			gt.Equal(t, v.Type, bigquery.TimestampFieldType)
		})
	})

	t.Run("time.Time pointer", func(t *testing.T) {
		row := struct {
			Time *time.Time
		}{
			Time: new(time.Time),
		}
		schema := gt.R1(bqs.Infer(row)).NoError(t)
		gt.A(t, schema).Length(1).At(0, func(t testing.TB, v *bigquery.FieldSchema) {
			gt.Equal(t, v.Type, bigquery.TimestampFieldType)
		})
	})

	t.Run("time.Time pointer with nil", func(t *testing.T) {
		row := struct {
			Time *time.Time
		}{}
		schema := gt.R1(bqs.Infer(row)).NoError(t)
		gt.A(t, schema).Length(0)
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
			gt.Equal(t, v.Type, bigquery.IntegerFieldType)
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
	gt.True(t, bqs.Equal(schemas, bigquery.Schema{
		{
			Name: "Int",
			Type: bigquery.IntegerFieldType,
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
	}))
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
					Type: bigquery.IntegerFieldType,
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
					Type: bigquery.IntegerFieldType,
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
				gt.True(t, bqs.Equal(schemas, tc.Expect))
			}
		})
	}
}

func TestInferArray(t *testing.T) {
	testCases := map[string]struct {
		input  any
		expect bigquery.Schema
	}{
		"string array": {
			input: struct {
				Str []string
			}{
				Str: []string{"a", "b"},
			},
			expect: bigquery.Schema{
				{
					Name:     "Str",
					Type:     bigquery.StringFieldType,
					Repeated: true,
				},
			},
		},
		"int array": {
			input: struct {
				Int []int
			}{
				Int: []int{1, 2},
			},
			expect: bigquery.Schema{
				{
					Name:     "Int",
					Type:     bigquery.IntegerFieldType,
					Repeated: true,
				},
			},
		},
		"nested array": {
			input: struct {
				Nest1 []struct {
					Str string

					Nest2 []struct {
						Int int
					}
				}
			}{
				Nest1: []struct {
					Str string

					Nest2 []struct {
						Int int
					}
				}{
					{
						Str: "a",
						Nest2: []struct {
							Int int
						}{
							{Int: 1},
							{Int: 2},
						},
					},
				},
			},
			expect: bigquery.Schema{
				{
					Name:     "Nest1",
					Type:     bigquery.RecordFieldType,
					Repeated: true,
					Schema: bigquery.Schema{
						{
							Name: "Str",
							Type: bigquery.StringFieldType,
						},
						{
							Name:     "Nest2",
							Type:     bigquery.RecordFieldType,
							Repeated: true,
							Schema: bigquery.Schema{
								{
									Name: "Int",
									Type: bigquery.IntegerFieldType,
								},
							},
						},
					},
				},
			},
		},
		"nested pointer array": {
			input: struct {
				Nest1 []*struct {
					Str string
				}
			}{
				Nest1: []*struct {
					Str string
				}{
					{Str: "a"},
					{Str: "b"},
				},
			},
			expect: bigquery.Schema{
				{
					Name:     "Nest1",
					Type:     bigquery.RecordFieldType,
					Repeated: true,
					Schema: bigquery.Schema{
						{
							Name: "Str",
							Type: bigquery.StringFieldType,
						},
					},
				},
			},
		},
		"invalid mixed array": {
			input: struct {
				Mixed []interface{}
			}{
				Mixed: []interface{}{"a", 1},
			},
			expect: nil,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			schemas, err := bqs.Infer(tc.input)
			if tc.expect == nil {
				gt.Error(t, err)
			} else {
				gt.NoError(t, err)
				gt.True(t, bqs.Equal(schemas, tc.expect))
			}
		})
	}
}

func TestInferMixIn(t *testing.T) {
	type Nest struct {
		Prev string
		Str  string
		Next string
	}
	type mix struct {
		Prev int
		Nest
		Next int
		Int  int
	}

	schemas := gt.R1(bqs.Infer(mix{})).NoError(t)
	gt.A(t, schemas).Length(4).
		At(0, func(t testing.TB, v *bigquery.FieldSchema) {
			gt.Equal(t, v.Name, "Prev")
			gt.Equal(t, v.Type, bigquery.IntegerFieldType)
		}).
		At(1, func(t testing.TB, v *bigquery.FieldSchema) {
			gt.Equal(t, v.Name, "Next")
			gt.Equal(t, v.Type, bigquery.IntegerFieldType)
		}).
		At(2, func(t testing.TB, v *bigquery.FieldSchema) {
			gt.Equal(t, v.Name, "Int")
			gt.Equal(t, v.Type, bigquery.IntegerFieldType)
		}).
		At(3, func(t testing.TB, v *bigquery.FieldSchema) {
			gt.Equal(t, v.Name, "Str")
			gt.Equal(t, v.Type, bigquery.StringFieldType)
		})
}

func TestTag(t *testing.T) {
	testCases := map[string]struct {
		input  any
		expect bigquery.Schema
	}{
		"tagged struct": {
			input: struct {
				Str string `bigquery:"blue"`
				Int int    `bigquery:"orange"`
			}{
				Str: "a",
				Int: 1,
			},
			expect: bigquery.Schema{
				{
					Name: "blue",
					Type: bigquery.StringFieldType,
				},
				{
					Name: "orange",
					Type: bigquery.IntegerFieldType,
				},
			},
		},
		"tagged nested struct": {
			input: struct {
				Nest struct {
					Str string `bigquery:"blue"`
					Int int    `bigquery:"orange"`
				}
			}{
				Nest: struct {
					Str string `bigquery:"blue"`
					Int int    `bigquery:"orange"`
				}{
					Str: "a",
					Int: 1,
				},
			},
			expect: bigquery.Schema{
				{
					Name: "Nest",
					Type: bigquery.RecordFieldType,
					Schema: bigquery.Schema{
						{
							Name: "blue",
							Type: bigquery.StringFieldType,
						},
						{
							Name: "orange",
							Type: bigquery.IntegerFieldType,
						},
					},
				},
			},
		},
		"skip field": {
			input: struct {
				Str string `bigquery:"-"`
				Int int    `bigquery:"orange"`
			}{
				Str: "a",
				Int: 1,
			},
			expect: bigquery.Schema{
				{
					Name: "orange",
					Type: bigquery.IntegerFieldType,
				},
			},
		},
		"prioritize bigquery tag than json": {
			input: struct {
				Str string `bigquery:"blue" json:"red"`
			}{
				Str: "a",
			},
			expect: bigquery.Schema{
				{
					Name: "blue",
					Type: bigquery.StringFieldType,
				},
			},
		},
		"use json tag if bigquery tag is not defined": {
			input: struct {
				Str string `json:"red"`
			}{
				Str: "a",
			},
			expect: bigquery.Schema{
				{
					Name: "red",
					Type: bigquery.StringFieldType,
				},
			},
		},
		"skip if bigquery has '-' tag even if having json tag": {
			input: struct {
				Str string `bigquery:"-" json:"red"`
			}{
				Str: "a",
			},
			expect: nil,
		},
		"do not skip even if having '-' tag in json": {
			input: struct {
				Str string `bigquery:"red" json:"-"`
			}{
				Str: "a",
			},
			expect: bigquery.Schema{
				{
					Name: "red",
					Type: bigquery.StringFieldType,
				},
			},
		},
		"ignore json omitempty tag": {
			input: struct {
				Str string `json:",omitempty"`
			}{
				Str: "a",
			},
			expect: bigquery.Schema{
				{
					Name: "Str",
					Type: bigquery.StringFieldType,
				},
			},
		},
		"Use only name part of json tag": {
			input: struct {
				Str string `json:"red,omitempty"`
			}{
				Str: "a",
			},
			expect: bigquery.Schema{
				{
					Name: "red",
					Type: bigquery.StringFieldType,
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			schemas, err := bqs.Infer(tc.input)
			gt.NoError(t, err)
			gt.True(t, bqs.Equal(schemas, tc.expect))
		})
	}
}

func TestEmptyStructField(t *testing.T) {
	s := struct {
		Str  string
		Time struct {
			time.Time
		}
	}{}

	schemas := gt.R1(bqs.Infer(s)).NoError(t)
	gt.A(t, schemas).Length(1)
	gt.Equal(t, schemas[0].Name, "Str")
}
