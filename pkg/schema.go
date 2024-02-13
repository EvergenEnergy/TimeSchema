package timestream

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"math/rand"
	"time"
)

// Schema represents a mapping from table names to measure names and then to
// a slice of metric names. It uses a generic type T, which must be a comparable
// type, allowing for flexibility in defining metric names.
type Schema[T comparable] map[string]map[string][]T

type invertedSchema[T comparable] map[T]struct {
	measureName string
	tableName   string
}

// TSSchema represents a Timestream schema. It provides methods to retrieve
// measure and table names for given metric names. It uses a generic type T
// for metric names, allowing the use of custom types as long as they are
// comparable.
type TSSchema[T comparable] struct {
	Schema         Schema[T]
	invertedSchema invertedSchema[T]
}

// NewTSSchema initialises a new TSSchema instance from the given Schema.
// The schema parameter is a mapping from table names to measure names and
// then to metric names of the generic type T.
func NewTSSchema[T comparable](schema Schema[T]) TSSchema[T] {
	return TSSchema[T]{Schema: schema, invertedSchema: invertSchema(schema)}
}

func invertSchema[T comparable](schema Schema[T]) invertedSchema[T] {
	inverted := make(invertedSchema[T])

	for tableName, measures := range schema {
		for measureName, metricNames := range measures {
			for _, metricName := range metricNames {
				inverted[metricName] = struct {
					measureName string
					tableName   string
				}{
					measureName: measureName,
					tableName:   tableName,
				}
			}
		}
	}
	return inverted
}

// GetMeasureNameFor retrieves the measure name associated with the given
// metric name. If the metric name is not found, it returns an error.
func (s TSSchema[T]) GetMeasureNameFor(metricName T) (string, error) {
	v, ok := s.invertedSchema[metricName]
	if !ok {
		return v.measureName, fmt.Errorf("metric name %T not found", metricName)
	}
	return v.measureName, nil
}

// GetTableNameFor retrieves the table name where the given metric name is
// stored. If the metric name is not found, it returns an error.
func (s TSSchema[T]) GetTableNameFor(metricName T) (string, error) {
	v, ok := s.invertedSchema[metricName]
	if !ok {
		return v.tableName, fmt.Errorf("metric name %T not found", metricName)
	}
	return v.tableName, nil
}

type PredefinedValues[T comparable] map[T]float64

// GenerateDummyData generates dummy data based on the schema structure.
// This function now uses the telemetry.MetricName type for keys.
func (t TSSchema[T]) GenerateDummyData(db string, time time.Time, predefinedValues PredefinedValues[T]) WriteRecords {
	var writeInputs []*timestreamwrite.WriteRecordsInput

	for tableName, measures := range t.Schema {
		writeInput := &timestreamwrite.WriteRecordsInput{
			DatabaseName: aws.String(db),
			CommonAttributes: &types.Record{
				MeasureValueType: types.MeasureValueTypeMulti,
				TimeUnit:         types.TimeUnitSeconds,
			},
		}
		var records []types.Record

		for measureName, metricNames := range measures {
			record := types.Record{
				MeasureName:      aws.String(fmt.Sprintf("%v", measureName)), // Convert measure name to *string
				MeasureValueType: types.MeasureValueTypeMulti,
				Time:             aws.String(fmt.Sprintf("%d", time.UnixMilli())),
			}
			measureValues := make([]types.MeasureValue, 0, len(metricNames))
			for _, metricName := range metricNames {
				var value string
				if predefinedValue, ok := predefinedValues[metricName]; ok {
					value = fmt.Sprintf("%f", predefinedValue) // Convert float64 to string
				} else {
					value = fmt.Sprintf("%f", rand.Float64()*100) // Adjust the range as needed and convert to string
				}
				measureValues = append(measureValues, types.MeasureValue{
					Name:  aws.String(fmt.Sprintf("%v", metricName)),
					Value: aws.String(value),
					Type:  types.MeasureValueTypeDouble,
				})

			}
			// Create a record for each metric.
			record.MeasureValues = measureValues
			records = append(records, record)
		}

		writeInput.TableName = aws.String(tableName)
		writeInput.Records = records
		writeInputs = append(writeInputs, writeInput)
	}
	return writeInputs
}

type WriteRecords []*timestreamwrite.WriteRecordsInput

func (w WriteRecords) RecordsForMeasure(measureName string) *timestreamwrite.WriteRecordsInput {
	for _, writeInput := range w {
		for _, record := range writeInput.Records {
			if *record.MeasureName == measureName {
				return writeInput
			}
		}
	}
	return nil
}
