package timestream_test

import (
	"fmt"
	"testing"

	timestream "github.com/EvergenEnergy/TimeSchema/pkg"
	"github.com/stretchr/testify/assert"
)

type testMeasureName string

func TestTSSchema_GetMeasureNameFor(t *testing.T) {
	type args struct {
		metricName string
	}
	tests := []struct {
		name   string
		schema timestream.Schema[string]
		args   args
		want   string
	}{
		{
			name:   "Test gets correct measure name",
			schema: timestream.Schema[string]{"table": {"measure": {"metric"}}},
			args: args{
				metricName: "metric",
			},
			want: "measure",
		},
		{
			name:   "Test gets correct measure name when multiple tables",
			schema: timestream.Schema[string]{"table": {"measure": {"metric"}}, "table2": {"measure2": {"metric2"}}},
			args: args{
				metricName: "metric",
			},
			want: "measure",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := timestream.NewTSSchema(tt.schema)
			got, err := s.GetMeasureNameFor(tt.args.metricName)
			assert.NoError(t, err)
			assert.Equalf(t, tt.want, got, "GetMeasureNameFor(%v)", tt.args.metricName)
		})
	}
}

func TestTSSchema_GetMeasureNameForCustomType(t *testing.T) {
	type args struct {
		metricName testMeasureName
	}
	tests := []struct {
		name   string
		schema timestream.Schema[testMeasureName]
		args   args
		want   string
	}{
		{
			name:   "Test gets correct measure name",
			schema: timestream.Schema[testMeasureName]{"table": {"measure": {"metric"}}},
			args: args{
				metricName: "metric",
			},
			want: "measure",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := timestream.NewTSSchema(tt.schema)
			got, err := s.GetMeasureNameFor(tt.args.metricName)
			assert.NoError(t, err)
			assert.Equalf(t, tt.want, got, "GetMeasureNameFor(%v)", tt.args.metricName)
		})
	}
}

func TestTSSchema_GetTableNameForCustomType(t *testing.T) {
	type args struct {
		metricName string
	}
	tests := []struct {
		name   string
		schema timestream.Schema[string]
		args   args
		want   string
	}{
		{
			name:   "Test gets correct Table name",
			schema: timestream.Schema[string]{"table": {"measure": {"metric"}}},
			args: args{
				metricName: "metric",
			},
			want: "table",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := timestream.NewTSSchema(tt.schema)
			got, err := s.GetTableNameFor(tt.args.metricName)
			assert.NoError(t, err)
			assert.Equalf(t, tt.want, got, "GetMeasureNameFor(%v)", tt.args.metricName)
		})
	}
}

func TestTSSchema_GetTableNameForFails(t *testing.T) {
	s := timestream.NewTSSchema(timestream.Schema[string]{"table": {"measure": {"metric"}}})
	_, err := s.GetTableNameFor("bad_metric")
	assert.Error(t, err)
}

func TestTSSchema_GetMeasureNameForReturnsErr(t *testing.T) {
	type args struct {
		metricName string
	}
	tests := []struct {
		name   string
		schema timestream.Schema[string]
		args   args
	}{
		{
			name:   "Test returns error on empty schema",
			schema: timestream.Schema[string]{},
			args: args{
				metricName: "metric",
			},
		},
		{
			name:   "Test returns error on empty table",
			schema: timestream.Schema[string]{"table": {}},
			args: args{
				metricName: "metric",
			},
		},
		{
			name:   "Test returns error on empty measure",
			schema: timestream.Schema[string]{"table": {"measure": {}}},
			args: args{
				metricName: "metric",
			},
		},
		{
			name:   "Test returns error on incorrect metric name",
			schema: timestream.Schema[string]{"table": {"measure": {"metric"}}},
			args: args{
				metricName: "bad_metric",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := timestream.NewTSSchema(tt.schema)
			_, err := s.GetMeasureNameFor(tt.args.metricName)
			assert.Error(t, err)
		})
	}
}

func TestTSSchema_GenerateDummyData(t1 *testing.T) {
	type args[T comparable] struct {
		predefinedValues timestream.PredefinedValues[string]
	}
	type testCase[T comparable] struct {
		name string
		t    timestream.TSSchema[T]
		args args[T]
		want map[string]map[string][]map[string]float64
	}
	tests := []testCase[string]{
		{
			name: "Test generates dummy data",
			t: timestream.NewTSSchema[string](
				timestream.Schema[string]{
					"table_1": {"measure_1": {"metric_1", "metric_2"}},
					"table_2": {
						"measure_2": {"metric_3", "metric_4"},
						"measure_3": {"metric_5", "metric_6", "metric_7"},
					},
				}),
			args: args[string]{predefinedValues: timestream.PredefinedValues[string]{
				"metric_1": 1,
				"metric_2": 2,
				"metric_3": 3,
				"metric_4": 4,
				"metric_5": 5,
				"metric_6": 6,
				"metric_7": 7,
			}},
			want: map[string]map[string][]map[string]float64{
				"table_1": {
					"measure_1": {{"metric_1": 1}, {"metric_2": 2}},
				},
				"table_2": {
					"measure_2": {{"metric_3": 3}, {"metric_4": 4}},
					"measure_3": {{"metric_5": 5}, {"metric_6": 6}, {"metric_7": 7}},
				},
			},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			assert.Equalf(t1, tt.want, tt.t.GenerateDummyData(tt.args.predefinedValues), "GenerateDummyData(%v)", tt.args.predefinedValues)
		})
	}
}

func TestTSSchema_GenerateDummyData_NoPredefinedValues(t *testing.T) {
	schema := timestream.Schema[string]{
		"table_1": {"measure_1": []string{"metric_1", "metric_2"}},
		"table_2": {
			"measure_2": []string{"metric_3", "metric_4"},
			"measure_3": []string{"metric_5", "metric_6", "metric_7"},
		},
	}
	tsSchema := timestream.NewTSSchema[string](schema)

	// No predefined values
	predefinedValues := timestream.PredefinedValues[string]{}

	got := tsSchema.GenerateDummyData(predefinedValues)

	// Assert that all expected tables, measures, and metrics exist and have generated data
	for tableName, measures := range schema {
		if _, ok := got[tableName]; !ok {
			t.Errorf("Missing table: %s", tableName)
			continue
		}

		for measureName, metricNames := range measures {
			generatedMeasure, ok := got[tableName][measureName]
			if !ok {
				t.Errorf("Missing measure: %s in table %s", measureName, tableName)
				continue
			}

			if len(generatedMeasure) != len(metricNames) {
				t.Errorf("Generated measure data length mismatch: got %d, want %d for measure %s in table %s", len(generatedMeasure), len(metricNames), measureName, tableName)
			}

			// Assert that there is some generated value for each metric
			for _, metricName := range metricNames {
				found := false
				for _, metricData := range generatedMeasure {
					if _, exists := metricData[fmt.Sprintf("%v", metricName)]; exists {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Missing generated data for metric %s in measure %s of table %s", metricName, measureName, tableName)
				}
			}
		}
	}
}
