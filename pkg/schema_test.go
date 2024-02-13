package timestream_test

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"testing"
	"time"

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
	now := time.Now()
	type args[T comparable] struct {
		dbName           string
		predefinedValues timestream.PredefinedValues[string]
	}
	type testCase[T comparable] struct {
		name string
		t    timestream.TSSchema[T]
		args args[T]
		want timestream.WriteRecords
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
			args: args[string]{dbName: "my-db", predefinedValues: timestream.PredefinedValues[string]{
				"metric_1": 1,
				"metric_2": 2,
				"metric_3": 3,
				"metric_4": 4,
				"metric_5": 5,
				"metric_6": 6,
				"metric_7": 7,
			}},
			want: timestream.WriteRecords{
				{
					DatabaseName: aws.String("my-db"),
					TableName:    aws.String("table_1"),
					CommonAttributes: &types.Record{
						MeasureValueType: types.MeasureValueTypeMulti,
						TimeUnit:         types.TimeUnitSeconds,
					},
					Records: []types.Record{
						{
							MeasureName:      aws.String("measure_1"),
							MeasureValueType: types.MeasureValueTypeMulti,
							MeasureValues: []types.MeasureValue{
								{
									Name:  aws.String("metric_1"),
									Type:  types.MeasureValueTypeDouble,
									Value: aws.String("1.000000"),
								},
								{
									Name:  aws.String("metric_2"),
									Type:  types.MeasureValueTypeDouble,
									Value: aws.String("2.000000"),
								},
							},
							Time: aws.String(fmt.Sprintf("%d", now.UnixMilli())),
						},
					},
				},
				{
					DatabaseName: aws.String("my-db"),
					TableName:    aws.String("table_2"),
					CommonAttributes: &types.Record{
						MeasureValueType: types.MeasureValueTypeMulti,
						TimeUnit:         types.TimeUnitSeconds,
					},
					Records: []types.Record{
						{
							MeasureName:      aws.String("measure_2"),
							MeasureValueType: types.MeasureValueTypeMulti,
							MeasureValues: []types.MeasureValue{
								{
									Name:  aws.String("metric_3"),
									Type:  types.MeasureValueTypeDouble,
									Value: aws.String("3.000000"),
								},
								{
									Name:  aws.String("metric_4"),
									Type:  types.MeasureValueTypeDouble,
									Value: aws.String("4.000000"),
								},
							},
							Time: aws.String(fmt.Sprintf("%d", now.UnixMilli())),
						},
						{
							MeasureName:      aws.String("measure_3"),
							MeasureValueType: types.MeasureValueTypeMulti,
							MeasureValues: []types.MeasureValue{
								{
									Name:  aws.String("metric_5"),
									Type:  types.MeasureValueTypeDouble,
									Value: aws.String("5.000000"),
								},
								{
									Name:  aws.String("metric_6"),
									Type:  types.MeasureValueTypeDouble,
									Value: aws.String("6.000000"),
								},
								{
									Name:  aws.String("metric_7"),
									Type:  types.MeasureValueTypeDouble,
									Value: aws.String("7.000000"),
								},
							},
							Time: aws.String(fmt.Sprintf("%d", now.UnixMilli())),
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			assert.Equalf(t1, tt.want, tt.t.GenerateDummyData(tt.args.dbName, now, tt.args.predefinedValues), "GenerateDummyData(%v)", tt.args.predefinedValues)
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

	got := tsSchema.GenerateDummyData("my_db", time.Now(), predefinedValues)

	// Assert that all expected tables, measures, and metrics exist and have generated data
	assert.Len(t, got, 2)
	assert.Equal(t, "table_1", *got[0].TableName)
	assert.Equal(t, "table_2", *got[1].TableName)
	assert.Len(t, got[0].Records, 1)
	assert.Len(t, got[1].Records, 2)
	assert.Len(t, got[0].Records[0].MeasureValues, 2)
	assert.Len(t, got[1].Records[0].MeasureValues, 2)
	assert.Len(t, got[1].Records[1].MeasureValues, 3)
	assert.NotNil(t, got[0].Records[0].MeasureValues[0].Value)
	assert.NotNil(t, got[0].Records[0].MeasureValues[1].Value)
}

func TestRecordsForMeasure(t *testing.T) {
	writeRecords := timestream.WriteRecords{
		{
			DatabaseName: aws.String("my-db"),
			TableName:    aws.String("table_1"),
			CommonAttributes: &types.Record{
				MeasureValueType: types.MeasureValueTypeMulti,
				TimeUnit:         types.TimeUnitSeconds,
			},
			Records: []types.Record{
				{
					MeasureName:      aws.String("measure_1"),
					MeasureValueType: types.MeasureValueTypeMulti,
					MeasureValues: []types.MeasureValue{
						{
							Name:  aws.String("metric_1"),
							Type:  types.MeasureValueTypeDouble,
							Value: aws.String("1.000000"),
						},
						{
							Name:  aws.String("metric_2"),
							Type:  types.MeasureValueTypeDouble,
							Value: aws.String("2.000000"),
						},
					},
					Time: aws.String(fmt.Sprintf("%d", now.UnixMilli())),
				},
			},
		},
		{
			DatabaseName: aws.String("my-db"),
			TableName:    aws.String("table_2"),
			CommonAttributes: &types.Record{
				MeasureValueType: types.MeasureValueTypeMulti,
				TimeUnit:         types.TimeUnitSeconds,
			},
			Records: []types.Record{
				{
					MeasureName:      aws.String("measure_2"),
					MeasureValueType: types.MeasureValueTypeMulti,
					MeasureValues: []types.MeasureValue{
						{
							Name:  aws.String("metric_3"),
							Type:  types.MeasureValueTypeDouble,
							Value: aws.String("3.000000"),
						},
						{
							Name:  aws.String("metric_4"),
							Type:  types.MeasureValueTypeDouble,
							Value: aws.String("4.000000"),
						},
					},
					Time: aws.String(fmt.Sprintf("%d", now.UnixMilli())),
				},
				{
					MeasureName:      aws.String("measure_3"),
					MeasureValueType: types.MeasureValueTypeMulti,
					MeasureValues: []types.MeasureValue{
						{
							Name:  aws.String("metric_5"),
							Type:  types.MeasureValueTypeDouble,
							Value: aws.String("5.000000"),
						},
						{
							Name:  aws.String("metric_6"),
							Type:  types.MeasureValueTypeDouble,
							Value: aws.String("6.000000"),
						},
						{
							Name:  aws.String("metric_7"),
							Type:  types.MeasureValueTypeDouble,
							Value: aws.String("7.000000"),
						},
					},
					Time: aws.String(fmt.Sprintf("%d", now.UnixMilli())),
				},
			},
		},
	}
	got := writeRecords.RecordsForMeasure("measure_1")

	assert.NotNil(t, got)
	assert.Len(t, got.Records, 1)
	assert.Equal(t, "measure_1", *got.Records[0].MeasureName)
	assert.Equal(t, "metric_1", *got.Records[0].MeasureValues[0].Name)
	assert.Equal(t, "1.000000", *got.Records[0].MeasureValues[0].Value)
	assert.Equal(t, "metric_2", *got.Records[0].MeasureValues[1].Name)
	assert.Equal(t, "2.000000", *got.Records[0].MeasureValues[1].Value)
}
