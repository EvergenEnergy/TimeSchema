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
type Schema[T1 comparable, T2 comparable] map[Table]map[MeasureName]Record[T1, T2]

type Table string
type MeasureName string
type Record[T1 comparable, T2 comparable] struct {
	Dimensions  []T1
	MetricNames []T2
}

type invertedSchema[T comparable] map[T]struct {
	measureName string
	tableName   string
}

// TSSchema represents a Timestream schema. It provides methods to retrieve
// measure and table names for given metric names. It uses a generic type T
// for metric names, allowing the use of custom types as long as they are
// comparable.
type TSSchema[T1 comparable, T2 comparable] struct {
	Schema         Schema[T1, T2]
	invertedSchema invertedSchema[T2]
}

// NewTSSchema initialises a new TSSchema instance from the given Schema.
// The schema parameter is a mapping from table names to measure names and
// then to metric names of the generic type T.
func NewTSSchema[T1 comparable, T2 comparable](schema Schema[T1, T2]) TSSchema[T1, T2] {
	return TSSchema[T1, T2]{Schema: schema, invertedSchema: invertSchema[T1, T2](schema)}
}

func invertSchema[T1 comparable, T2 comparable](schema Schema[T1, T2]) invertedSchema[T2] {
	inverted := make(invertedSchema[T2])

	for tableName, measures := range schema {
		for measureName, records := range measures {
			for _, metricName := range records.MetricNames {
				inverted[metricName] = struct {
					measureName string
					tableName   string
				}{
					measureName: string(measureName),
					tableName:   string(tableName),
				}
			}
		}
	}
	return inverted
}

// GetMeasureNameFor retrieves the measure name associated with the given
// metric name. If the metric name is not found, it returns an error.
func (s TSSchema[T1, T2]) GetMeasureNameFor(metricName T2) (string, error) {
	v, ok := s.invertedSchema[metricName]
	if !ok {
		return v.measureName, fmt.Errorf("metric name %T not found", metricName)
	}
	return v.measureName, nil
}

// GetTableNameFor retrieves the table name where the given metric name is
// stored. If the metric name is not found, it returns an error.
func (s TSSchema[T1, T2]) GetTableNameFor(metricName T2) (string, error) {
	v, ok := s.invertedSchema[metricName]
	if !ok {
		return v.tableName, fmt.Errorf("metric name %T not found", metricName)
	}
	return v.tableName, nil
}

type PredefinedValues[T comparable] map[T]float64

// GenerateDummyData generates dummy data based on the schema structure.
func (t TSSchema[T1, T2]) GenerateDummyData(db string, time time.Time, predefinedValues PredefinedValues[T2]) WriteRecords {
	var writeInputs []*timestreamwrite.WriteRecordsInput

	for tableName, measures := range t.Schema {
		writeInput := &timestreamwrite.WriteRecordsInput{
			DatabaseName: aws.String(db),
			CommonAttributes: &types.Record{
				MeasureValueType: types.MeasureValueTypeMulti,
				TimeUnit:         types.TimeUnitMilliseconds,
			},
		}
		var records []types.Record

		for measureName, metricNames := range measures {
			record := types.Record{
				MeasureName:      aws.String(fmt.Sprintf("%v", measureName)), // Convert measure name to *string
				MeasureValueType: types.MeasureValueTypeMulti,
				Time:             aws.String(fmt.Sprintf("%d", time.UnixMilli())),
			}
			measureValues := make([]types.MeasureValue, 0, len(metricNames.MetricNames))
			for _, metricName := range metricNames.MetricNames {
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
			dimensionValues := make([]types.Dimension, 0, len(metricNames.Dimensions))
			for _, dimensionName := range metricNames.Dimensions {
				dimensionValues = append(dimensionValues, types.Dimension{
					Name:  aws.String(fmt.Sprintf("%v", dimensionName)),
					Value: aws.String("dummy"),
				})
			}
			// Create a record for each metric.
			record.MeasureValues = measureValues
			record.Dimensions = dimensionValues
			records = append(records, record)
		}

		writeInput.TableName = aws.String(string(tableName))
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
