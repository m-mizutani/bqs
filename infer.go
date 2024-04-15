package bqs

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
)

// Infer infers the schema of the data and returns a bigquery.Schema. It can infer the schema of nested structs and maps.
func Infer(data any) (bigquery.Schema, error) {
	return inferObject(reflect.ValueOf(data))
}

func inferObject(data reflect.Value) (bigquery.Schema, error) {
	var schema bigquery.Schema
	var embedded bigquery.Schema

	switch data.Kind() {
	case reflect.Ptr, reflect.Interface:
		if data.IsNil() {
			value := reflect.New(data.Type().Elem())
			return inferObject(value)
		}
		return inferObject(data.Elem())

	case reflect.Struct:
		for i := 0; i < data.NumField(); i++ {
			field := data.Field(i)
			if !field.CanInterface() {
				continue
			}

			fieldInfo := data.Type().Field(i)
			if fieldInfo.Anonymous {
				resp, err := inferObject(field)
				if err != nil {
					return nil, err
				}
				embedded = append(embedded, resp...)
				continue
			}

			jsonTag := strings.Split(fieldInfo.Tag.Get("json"), ",")[0]
			bqTag := fieldInfo.Tag.Get("bigquery")

			var name string
			switch {
			case bqTag == "-":
				continue
			case bqTag != "":
				name = bqTag
			case jsonTag == "-":
				continue
			case jsonTag != "":
				name = jsonTag
			default:
				name = fieldInfo.Name
			}

			fieldSchema, err := inferField(name, field)
			if err != nil {
				return nil, err
			}
			if fieldSchema != nil {
				schema = append(schema, fieldSchema)
			}
		}

	case reflect.Map:
		for _, key := range data.MapKeys() {
			value := data.MapIndex(key)
			if !value.CanInterface() {
				continue
			}
			if key.Kind() != reflect.String {
				return nil, fmt.Errorf("invalid key type: %v: %w", key.Kind(), ErrUnsupportedKeyType)
			}

			fieldSchema, err := inferField(key.String(), value)
			if err != nil {
				return nil, err
			}
			if fieldSchema != nil {
				schema = append(schema, fieldSchema)
			}
		}

	default:
		return nil, fmt.Errorf("invalid data: %v: %w", data.Kind(), ErrUnsupportedObject)
	}

	for _, field := range embedded {
		var found bool
		for _, f := range schema {
			if f.Name == field.Name {
				found = true
				break
			}
		}
		if !found {
			schema = append(schema, field)
		}
	}

	return schema, nil
}

func inferField(name string, data reflect.Value) (*bigquery.FieldSchema, error) {
	kind := data.Kind()
	switch kind {
	case reflect.Ptr, reflect.Interface:
		if data.IsNil() {
			value := reflect.New(data.Type().Elem())
			return inferField(name, value)
		}
		return inferField(name, data.Elem())

	case reflect.String:
		return &bigquery.FieldSchema{
			Name: name,
			Type: bigquery.StringFieldType,
		}, nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return &bigquery.FieldSchema{
			Name: name,
			Type: bigquery.IntegerFieldType,
		}, nil

	case reflect.Float32, reflect.Float64:
		return &bigquery.FieldSchema{
			Name: name,
			Type: bigquery.FloatFieldType,
		}, nil

	case reflect.Bool:
		return &bigquery.FieldSchema{
			Name: name,
			Type: bigquery.BooleanFieldType,
		}, nil

	case reflect.Struct, reflect.Map:
		// if data is time.Time, then it should be a TIMESTAMP
		timeType := reflect.TypeOf(time.Time{})
		if kind == reflect.Struct && data.Type().ConvertibleTo(timeType) {
			return &bigquery.FieldSchema{
				Name: name,
				Type: bigquery.TimestampFieldType,
			}, nil
		}

		schema, err := inferObject(data)
		if err != nil {
			return nil, err
		}
		if len(schema) == 0 {
			return nil, nil
		}

		return &bigquery.FieldSchema{
			Name:   name,
			Type:   bigquery.RecordFieldType,
			Schema: schema,
		}, nil

	case reflect.Slice, reflect.Array:
		if data.Len() == 0 {
			elem := reflect.New(data.Type().Elem()).Elem()
			if elem.Kind() == reflect.Interface {
				return nil, nil
			}

			schema, err := inferField(name, elem)
			if err != nil {
				return nil, err
			}
			if schema == nil {
				return nil, nil
			}
			schema.Repeated = true
			return schema, nil
		}

		var field *bigquery.FieldSchema
		for i := 0; i < data.Len(); i++ {
			elem := data.Index(i)
			if !elem.CanInterface() {
				continue
			}

			newField, err := inferField(name, elem)
			if err != nil {
				return nil, err
			}

			if field == nil {
				field = newField
				continue
			} else {
				if newField.Type != field.Type {
					return nil, fmt.Errorf("type conflict in array: %s: %w", name, ErrConflictField)
				}
				if newField.Schema != nil {
					merged, err := Merge(field.Schema, newField.Schema)
					if err != nil {
						return nil, err
					}
					field.Schema = merged
				}
			}
		}
		field.Repeated = true
		return field, nil

	default:
		return nil, fmt.Errorf("invalid data type: %v: %w", data.Kind(), ErrUnsupportedDataType)
	}
}
