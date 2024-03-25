// Package timestream provides a comprehensive set of tools for interacting with AWS Timestream.
// It includes functionalities for schema definition and management, as well as efficient
// marshalling and unmarshalling of data for AWS Timestream. The package is designed to be generic,
// allowing for flexible data types and simplifying the process of preparing data for Timestream
// as well as retrieving and interpreting query results.
package timestream

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
)

type requiredField string

const (
	measure   requiredField = "measure"
	timestamp requiredField = "timestamp"
	dimension requiredField = "dimension"
	attribute requiredField = "attribute"
)

// Marshal takes a struct as input and transforms it into a types.Record
// compatible with AWS Timestream. The struct fields should be annotated
// with 'timestream' tags to indicate how they map to the Timestream data model.
//
// Supported tag options:
//   - "timestamp": Indicates the field representing the timestamp for the record.
//     The field must be of type time.Time and non-zero.
//   - "measure": Represents the measure name. It must be a non-empty string.
//   - "dimension": Used for dimensions in Timestream. Multiple dimensions are supported.
//     Optionally, a 'name' can be specified (e.g., `timestream:"dimension,name=customName"`).
//   - "attribute": Represents measure values. Multiple measure values are supported.
//     The field can be of a primitive type (string, int, float).
//     For `time.Time` fields, you can specify the unit of time (s for seconds, ms for milliseconds, ns for nanoseconds)
//     to format the timestamp accordingly, e.g., `timestream:"attribute,name=timestamp,unit=ms"`.
//   - "omitempty": This tag can only be applied to string fields. Fields with this tag
//     are omitted if they are empty strings. For non-string fields, this tag will
//     cause an error during marshalling. It is intended to reduce data size and handle
//     optional string fields gracefully.
//
// The function returns an error if the input is not a struct,
// does not meet the tagging requirements, or if any fields are of unsupported types.
//
// Examples of struct field tags and their meanings:
//
//	type MyData struct {
//	    Timestamp   time.Time `timestream:"timestamp"`
//	    SensorName  string    `timestream:"measureName"`
//	    Location    string    `timestream:"dimension,name=location"`
//	    Temperature float64   `timestream:"attribute,name=temperature,omitempty"`
//		EventTime   time.Time `timestream:"attribute,name=eventTime,unit=ms"`
//	}
//
// Note: This function uses reflection to inspect struct fields. Fields with unsupported
// types or incorrect tagging will result in an error.
//
// The function is designed to handle common use cases efficiently, but complex structs
// with deeply nested structures or a large number of fields may impact performance.
// Limitations:
// - The function does not support encoding of channel, complex, function values,
// or cyclic data structures. Attempting to encode such values will result in an error.
// - The function currently only supports basic types and time.Time for measure values.
// Custom types or types implementing specific interfaces are not currently supported.
// - There is a limitation in the depth of struct traversal; only the first level of fields
// is considered. Nested structs or embedded structs are not recursively processed.
//
// It's important to ensure that structs passed to Marshal are well-formed according to
// the expectations of AWS Timestream data model, particularly regarding the types and
// formatting of timestamps, measure names, dimensions, and attributes.
// Example usage:
//
//	data := MyData{
//	    Time:        time.Now(),
//	    SensorName:  "Sensor1",
//	    Location:    "Room1",
//	    Temperature: 23.5,
//		EventTime:   time.Now(),
//	}
//	record, err := Marshal(data)
//	if err != nil {
//	    // handle error
//	}
//	// use record with AWS Timestream
//
// This function is part of a package designed to simplify the interaction with AWS Timestream,
// making the process of data preparation more straightforward and less error-prone.
func Marshal(v any) ([]types.Record, error) {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Slice {
		var records []types.Record

		var errs error

		for i := 0; i < val.Len(); i++ {
			record, err := marshalSingle(val.Index(i).Interface())
			if err != nil {
				errs = errors.Join(errs, err)
				continue
			}

			records = append(records, record)
		}
		if errs != nil {
			return nil, errs
		}
		return records, nil
	}

	record, err := marshalSingle(v)
	if err != nil {
		return nil, err
	}
	return []types.Record{record}, err
}

func marshalSingle(v any) (types.Record, error) {
	val, err := validateRequiredFields(v)
	if err != nil {
		return types.Record{}, fmt.Errorf("invalid struct, %w", err)
	}

	var record types.Record

	for i := 0; i < val.NumField(); i++ {
		tag, ok := val.Type().Field(i).Tag.Lookup("timestream")
		if !ok {
			continue
		}

		err = handleRecord(&record, val, i, tag)
		if err != nil {
			return types.Record{}, err
		}
	}
	return record, nil
}

func handleRecord(record *types.Record, val reflect.Value, i int, tag string) error {
	field := val.Type().Field(i)
	tagParts := strings.Split(tag, ",")
	tagName, omitempty := extractTagName(field, tagParts)
	tagType := requiredField(tagParts[0])

	switch tagType {
	case timestamp:
		timestamp, ok := val.Field(i).Interface().(time.Time)
		if !ok {
			return fmt.Errorf("timestamp field is not a time.Time")
		}

		formattedTime := fmt.Sprintf("%d", timestamp.UnixMilli())
		record.Time = &formattedTime
	case measure:
		measureName := val.Field(i).Interface().(string)
		record.MeasureName = &measureName
	case dimension:
		dimensionName := val.Field(i).Interface().(string)
		record.Dimensions = append(record.Dimensions, types.Dimension{Name: &tagName, Value: aws.String(dimensionName)})
	case attribute:
		measureValue, err := handleMeasureValue(tagName, tag, val.Field(i), omitempty)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(measureValue, types.MeasureValue{}) {
			record.MeasureValues = append(record.MeasureValues, measureValue)
		}
	}
	return nil
}

func handleMeasureValue(tagName, tag string, fieldValue reflect.Value, omitEmpty bool) (types.MeasureValue, error) {
	var measureValue types.MeasureValue

	// Check for zero value and omitEmpty
	if omitEmpty && isZeroValue(fieldValue) {
		return types.MeasureValue{}, nil // Special error or value indicating to skip
	}

	measureValue.Name = aws.String(tagName)

	switch fieldValue.Kind() {
	case reflect.Struct:
		// Check specifically for time.Time
		if fieldValue.Type() == reflect.TypeOf(time.Time{}) {
			timeValue, ok := fieldValue.Interface().(time.Time)
			if !ok {
				return types.MeasureValue{}, fmt.Errorf("field is not a time.Time")
			}
			// Extract unit from tag, default to milliseconds
			unit := "s"
			tagParts := strings.Split(tag, ",")
			for _, part := range tagParts {
				if strings.HasPrefix(part, "unit=") {
					unit = strings.TrimPrefix(part, "unit=")
					break
				}
			}

			// Convert time based on unit
			switch unit {
			case "s":
				measureValue.Value = aws.String(strconv.FormatInt(timeValue.Unix(), 10))
			case "ms":
				measureValue.Value = aws.String(strconv.FormatInt(timeValue.UnixMilli(), 10))
			case "ns":
				measureValue.Value = aws.String(strconv.FormatInt(timeValue.UnixNano(), 10))
			default:
				return types.MeasureValue{}, fmt.Errorf("unsupported unit for time: %s", unit)
			}

			measureValue.Type = types.MeasureValueTypeTimestamp
			return measureValue, nil
		} else {
			return types.MeasureValue{}, fmt.Errorf("unsupported struct type for measureValue")
		}
	case reflect.String:
		strValue := fieldValue.String()
		if strValue == "" {
			// Replace empty string with "-"
			measureValue.Value = aws.String("-")
		} else {
			measureValue.Value = aws.String(strValue)
		}

		measureValue.Type = types.MeasureValueTypeVarchar
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		formatInt := strconv.FormatInt(fieldValue.Int(), 10)
		measureValue.Value = &formatInt
		measureValue.Type = types.MeasureValueTypeBigint
	case reflect.Float32, reflect.Float64:
		measureValue.Value = aws.String(fmt.Sprintf("%f", fieldValue.Float()))
		measureValue.Type = types.MeasureValueTypeDouble
	default:
		return types.MeasureValue{}, fmt.Errorf("unsupported type for measureValue")
	}

	return measureValue, nil
}

func isZeroValue(v reflect.Value) bool {
	// Check if the value is a string
	if v.Kind() == reflect.String {
		// Return true if the string is empty
		return v.Len() == 0
	}

	// For all other types, return false
	return false
}

func extractTagName(field reflect.StructField, tagParts []string) (string, bool) {
	tagName := field.Name
	omitEmpty := false

	for _, part := range tagParts {
		if part == "omitempty" {
			omitEmpty = true
		} else if strings.HasPrefix(part, "name=") {
			tagName = strings.TrimPrefix(part, "name=")
		}
	}
	return tagName, omitEmpty
}

func validateRequiredFields(v any) (reflect.Value, error) {
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Struct {
		return reflect.Value{}, fmt.Errorf("input is not a struct")
	}

	requiredTags := map[requiredField]int{
		measure:   0,
		timestamp: 0,
		dimension: 0,
		attribute: 0,
	}

	err := validateTypes(val, requiredTags)
	if err != nil {
		return reflect.Value{}, err
	}

	err = validateAppearances(requiredTags)
	if err != nil {
		return reflect.Value{}, err
	}
	return val, nil
}

func validateAppearances(requiredTags map[requiredField]int) error {
	for tag, count := range requiredTags {
		if count == 0 {
			return fmt.Errorf("missing required tag: %s", tag)
		}
		// Assuming multiple dimensions and measureValues are allowed
		if count > 1 && tag != dimension && tag != attribute {
			return fmt.Errorf("tag %s appears more than once", tag)
		}
	}
	return nil
}

func validateTypes(val reflect.Value, requiredTags map[requiredField]int) error {
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := val.Type().Field(i)

		if err := validateField(field, fieldType, requiredTags); err != nil {
			return err
		}
	}
	return nil
}

func validateField(field reflect.Value, fieldType reflect.StructField, requiredTags map[requiredField]int) error {
	tag, ok := fieldType.Tag.Lookup("timestream")
	if !ok {
		return nil
	}

	tagParts := strings.Split(tag, ",")
	if err := checkOmitEmpty(fieldType, tagParts); err != nil {
		return err
	}

	requiredTags[requiredField(strings.Split(tag, ",")[0])]++

	if err := checkFieldAccessibility(field, fieldType); err != nil {
		return err
	}

	return validateFieldTypeBasedOnTag(field, tag)
}

func checkOmitEmpty(fieldType reflect.StructField, tagParts []string) error {
	_, omitEmpty := extractTagName(fieldType, tagParts)
	if omitEmpty && fieldType.Type.Kind() != reflect.String {
		return fmt.Errorf("omitempty can only be used with string fields, found in field '%s'", fieldType.Name)
	}
	return nil
}

func checkFieldAccessibility(field reflect.Value, fieldType reflect.StructField) error {
	if !field.CanInterface() {
		return fmt.Errorf("field %s is not accessible, needs to be public", fieldType.Name)
	}
	return nil
}

func validateFieldTypeBasedOnTag(field reflect.Value, tag string) error {
	switch tag {
	case string(timestamp):
		return validateTimestampField(field)
	case string(measure):
		return validateMeasureField(field)
	}
	return nil
}

func validateTimestampField(field reflect.Value) error {
	timestamp, ok := field.Interface().(time.Time)
	if !ok || timestamp.IsZero() {
		return fmt.Errorf("timestamp field is either not a time.Time or has a zero value")
	}
	return nil
}

func validateMeasureField(field reflect.Value) error {
	measureName, ok := field.Interface().(string)
	if !ok || measureName == "" {
		return fmt.Errorf("measureName field is either not a string or has a zero value")
	}
	return nil
}
