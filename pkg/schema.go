package timestream

import (
	"fmt"
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
