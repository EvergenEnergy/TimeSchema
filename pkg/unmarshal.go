package timestream

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
)

// Unmarshal decodes data from Timestream query output into a struct or a slice of structs.
//
// The 'v' parameter must be a pointer to a struct or a pointer to a slice of structs.
// The struct fields should be annotated with 'timestream' tags that specify how to map
// Timestream column names to struct fields. Supported struct field types are string, int,
// float64, and time.Time.
//
// The function supports unmarshalling into either a single struct (if the query output
// contains a single row of data) or a slice of structs (if multiple rows are present).
// Each struct field's tag should match the Timestream column name, e.g.,
// `timestream:"name=column_name"` for regular columns or `timestream:"time"` for the
// special timestamp column.
//
// Example usage:
//
//	type MyData struct {
//	    Timestamp time.Time `timestream:"time"`
//	    Name      string    `timestream:"name=dimension_name"`
//	    Energy    float64   `timestream:"name=modelled_generation"`
//	    Power     int       `timestream:"name=actual_pv_power"`
//	}
//
// var myData MyData
// err := Unmarshal(queryOutput, &myData)
//
//	if err != nil {
//	    // handle error
//	}
//
// var myDataSlice []MyData
// err = Unmarshal(queryOutput, &myDataSlice)
//
//	if err != nil {
//	    // handle error
//	}
//
// This function will return an error if:
// - The 'v' parameter is not a pointer.
// - The 'v' parameter is not a pointer to a struct or a slice of structs.
// - The length of the slice does not match the number of rows in the query output (when unmarshaling into a slice).
// - There is a mismatch between the number of columns in the query output and the number of fields in the struct.
//
// Note: It's important to ensure that the types of the struct fields are compatible with the data types
// in the Timestream query output. For example, Timestream timestamps should be mapped to time.Time fields,
// and integers or floats in Timestream should be mapped to int or float64 fields in the struct, respectively.
func Unmarshal(queryOutput *timestreamquery.QueryOutput, v any) error {
	structVal, err := validateInput(queryOutput, v)
	if err != nil {
		return err
	}

	lookup := buildLookupTable(queryOutput.ColumnInfo)

	if structVal.Kind() == reflect.Slice {
		sliceType := structVal.Type().Elem()
		resizedSlice := reflect.MakeSlice(structVal.Type(), len(queryOutput.Rows), len(queryOutput.Rows))

		for i, row := range queryOutput.Rows {
			newStruct := reflect.New(sliceType).Elem()
			if err := unmarshalRow(row, newStruct, lookup); err != nil {
				return err
			}

			resizedSlice.Index(i).Set(newStruct)
		}

		structVal.Set(resizedSlice)
	} else if len(queryOutput.Rows) == 1 {
		if err := unmarshalRow(queryOutput.Rows[0], structVal, lookup); err != nil {
			return err
		}
	}

	return nil
}

func unmarshalRow(row types.Row, structVal reflect.Value, lookup map[string]int) error {
	t := structVal.Type()
	for i := 0; i < structVal.NumField(); i++ {
		field := t.Field(i)

		tag := field.Tag.Get("timestream")
		if tag == "" || tag == "-" {
			continue
		}

		columnName, err := getColumnName(tag)
		if err != nil {
			return err
		}

		pos, found := lookup[columnName]
		if !found {
			return fmt.Errorf("column '%s' not found in Timestream data", columnName)
		}

		if err := setStructFieldFromRow(row, pos, structVal.Field(i)); err != nil {
			return err
		}
	}
	return nil
}

func getColumnName(tag string) (string, error) {
	if tag == "time" || tag == "timestamp" {
		return tag, nil
	}

	tagParts := strings.Split(tag, "=")
	if len(tagParts) != 2 || tagParts[0] != "name" {
		return "", fmt.Errorf("invalid tag format")
	}

	return tagParts[1], nil
}

func setStructFieldFromRow(row types.Row, pos int, field reflect.Value) error {
	if pos < 0 || pos >= len(row.Data) {
		return fmt.Errorf("column position '%d' out of range", pos)
	}

	data := row.Data[pos].ScalarValue
	if data == nil {
		return nil // field remains at its zero value
	}

	return setFieldValue(field, *data)
}

func setFieldValue(field reflect.Value, data string) error {
	switch field.Kind() {
	case reflect.String:
		field.SetString(data)
	case reflect.Int, reflect.Int64:
		intValue, err := strconv.ParseInt(data, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse int: %w", err)
		}

		field.SetInt(intValue)
	case reflect.Float64:
		floatValue, err := strconv.ParseFloat(data, 64)
		if err != nil {
			return fmt.Errorf("failed to parse float64: %w", err)
		}

		field.SetFloat(floatValue)
	case reflect.Struct:
		// Assuming the field is time.Time and the custom format matches your timestamp
		const customLayout = "2006-01-02 15:04:05.000000000"

		parsedTime, err := time.Parse(customLayout, data)
		if err != nil {
			return fmt.Errorf("failed to parse time: %w", err)
		}

		field.Set(reflect.ValueOf(parsedTime))
	default:
		return fmt.Errorf("setFieldValue: unhandled field type: %s", field.Kind())
	}
	return nil
}

func validateInput(queryOutput *timestreamquery.QueryOutput, v any) (reflect.Value, error) {
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Ptr {
		return reflect.Value{}, fmt.Errorf("target must be a pointer, got %s", val.Kind().String())
	}

	if queryOutput == nil {
		return reflect.Value{}, fmt.Errorf("queryOutput is nil")
	}

	valElem := val.Elem()
	if err := validateTargetType(valElem); err != nil {
		return reflect.Value{}, err
	}

	if err := validateRowCount(valElem, queryOutput.Rows); err != nil {
		return reflect.Value{}, err
	}

	if err := validateRowDataLength(queryOutput); err != nil {
		return reflect.Value{}, err
	}

	return valElem, nil
}

func validateTargetType(valElem reflect.Value) error {
	switch valElem.Kind() {
	case reflect.Slice, reflect.Struct:
		return nil
	default:
		return fmt.Errorf("target must be a pointer to a struct or slice of structs, got %s", valElem.Kind().String())
	}
}

func validateRowCount(valElem reflect.Value, rows []types.Row) error {
	if valElem.Kind() == reflect.Slice && valElem.Len() > 0 && len(rows) != valElem.Len() {
		return fmt.Errorf("queryOutput and target slice length mismatch")
	}
	if valElem.Kind() == reflect.Struct && len(rows) > 1 {
		return fmt.Errorf("expected a slice for a multiple rows QueryResult")
	}
	return nil
}

func validateRowDataLength(queryOutput *timestreamquery.QueryOutput) error {
	for _, row := range queryOutput.Rows {
		if len(row.Data) != len(queryOutput.ColumnInfo) {
			return fmt.Errorf("mismatched length of row data and column info")
		}
	}
	return nil
}

func buildLookupTable(columnInfo []types.ColumnInfo) map[string]int {
	lookup := make(map[string]int)
	for i, column := range columnInfo {
		lookup[*column.Name] = i
	}
	return lookup
}
