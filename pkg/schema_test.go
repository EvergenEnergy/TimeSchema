package timestream_test

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	timestream "github.com/EvergenEnergy/TimeSchema/pkg"
	"github.com/stretchr/testify/assert"
)

type (
	testMetricName string
	testDimension  string
)

func TestTSSchema_GetMeasureNameFor(t *testing.T) {
	type args struct {
		metricName string
	}
	tests := []struct {
		name   string
		schema timestream.Schema[string, string]
		args   args
		want   string
	}{
		{
			name: "Test gets correct measure name",
			schema: timestream.Schema[string, string]{"table": {
				"measure": {
					Dimensions:  []string{},
					MetricNames: []string{"metric"},
				},
			}},
			args: args{
				metricName: "metric",
			},
			want: "measure",
		},
		{
			name: "Test gets correct measure name when multiple tables",
			schema: timestream.Schema[string, string]{
				"table": {"measure": {
					Dimensions:  []string{},
					MetricNames: []string{"metric"},
				}},
				"table2": {"measure2": {
					MetricNames: []string{"metric2"},
				}},
			},
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
		metricName testMetricName
	}
	tests := []struct {
		name   string
		schema timestream.Schema[testDimension, testMetricName]
		args   args
		want   string
	}{
		{
			name:   "Test gets correct measure name",
			schema: timestream.Schema[testDimension, testMetricName]{"table": {"measure": {MetricNames: []testMetricName{"metric"}}}},
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
		schema timestream.Schema[string, string]
		args   args
		want   string
	}{
		{
			name:   "Test gets correct Table name",
			schema: timestream.Schema[string, string]{"table": {"measure": {MetricNames: []string{"metric"}}}},
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
	s := timestream.NewTSSchema(timestream.Schema[string, string]{"table": {"measure": {MetricNames: []string{"metric"}}}})
	_, err := s.GetTableNameFor("bad_metric")
	assert.Error(t, err)
}

func TestTSSchema_GetMeasureNameForReturnsErr(t *testing.T) {
	type args struct {
		metricName string
	}
	tests := []struct {
		name   string
		schema timestream.Schema[string, string]
		args   args
	}{
		{
			name:   "Test returns error on empty schema",
			schema: timestream.Schema[string, string]{},
			args: args{
				metricName: "metric",
			},
		},
		{
			name:   "Test returns error on empty table",
			schema: timestream.Schema[string, string]{"table": {}},
			args: args{
				metricName: "metric",
			},
		},
		{
			name:   "Test returns error on empty measure",
			schema: timestream.Schema[string, string]{"table": {"measure": {}}},
			args: args{
				metricName: "metric",
			},
		},
		{
			name:   "Test returns error on incorrect metric name",
			schema: timestream.Schema[string, string]{"table": {"measure": {MetricNames: []string{"metric"}}}},
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
	type testCase[T1 comparable, T2 comparable] struct {
		name string
		t    timestream.TSSchema[T1, T2]
		args args[T2]
		want timestream.WriteRecords
	}
	tests := []testCase[string, string]{
		{
			name: "Test generates dummy data",
			t: timestream.NewTSSchema[string, string](
				timestream.Schema[string, string]{
					"table_1": {"measure_1": {Dimensions: []string{"dimension_1"}, MetricNames: []string{"metric_1", "metric_2"}}},
					"table_2": {
						"measure_2": {Dimensions: []string{"dimension_2", "dimension_3"}, MetricNames: []string{"metric_3", "metric_4"}},
						"measure_3": {Dimensions: []string{"dimension_4"}, MetricNames: []string{"metric_5", "metric_6", "metric_7"}},
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
						TimeUnit:         types.TimeUnitMilliseconds,
					},
					Records: []types.Record{
						{
							Dimensions: []types.Dimension{
								{
									Name:  aws.String("dimension_1"),
									Value: aws.String("dummy"),
								},
							},
							MeasureName: aws.String("measure_1"),
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
						TimeUnit:         types.TimeUnitMilliseconds,
					},
					Records: []types.Record{
						{
							Dimensions: []types.Dimension{
								{
									Name:  aws.String("dimension_2"),
									Value: aws.String("dummy"),
								},
								{
									Name:  aws.String("dimension_3"),
									Value: aws.String("dummy"),
								},
							},
							MeasureName: aws.String("measure_2"),
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
							Dimensions: []types.Dimension{
								{
									Name:  aws.String("dimension_4"),
									Value: aws.String("dummy"),
								},
							},
							MeasureName: aws.String("measure_3"),
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
			unexported := cmpopts.IgnoreUnexported(types.Record{}, types.Dimension{}, types.MeasureValue{}, timestreamwrite.WriteRecordsInput{})

			got := tt.t.GenerateDummyData(tt.args.dbName, now, tt.args.predefinedValues)
			// First, sort the `got` slice by TableName.
			sort.Slice(got, func(i, j int) bool {
				return *got[i].TableName < *got[j].TableName
			})

			// Then, sort each `WriteRecordsInput`'s Records slice by MeasureName.
			for _, writeRecordsInput := range got {
				sort.Slice(writeRecordsInput.Records, func(i, j int) bool {
					return *writeRecordsInput.Records[i].MeasureName < *writeRecordsInput.Records[j].MeasureName
				})
			}
			assert.Len(t1, got, 2)
			if diff := cmp.Diff(tt.want, got, unexported); diff != "" {
				t1.Errorf("Mismatch (-expected +actual):\n%s", diff)
			}
		})
	}
}

func TestTSSchema_GenerateDummyData_NoPredefinedValues(t *testing.T) {
	schema := timestream.Schema[string, string]{
		"table_1": {"measure_1": {MetricNames: []string{"metric_1", "metric_2"}}},
		"table_2": {
			"measure_2": {MetricNames: []string{"metric_3", "metric_4"}},
			"measure_3": {MetricNames: []string{"metric_5", "metric_6", "metric_7"}},
		},
	}
	tsSchema := timestream.NewTSSchema[string](schema)

	// No predefined values
	predefinedValues := timestream.PredefinedValues[string]{}

	got := tsSchema.GenerateDummyData("my_db", time.Now(), predefinedValues)

	// Assert that all expected tables, measures, and metrics exist and have generated data
	expectedMetricsCount := map[string]int{
		"table_1_measure_1": 2, // 2 metrics under measure_1 in table_1
		"table_2_measure_2": 2, // 2 metrics under measure_2 in table_2
		"table_2_measure_3": 3, // 3 metrics under measure_3 in table_2
	}

	gotMetricsCount := make(map[string]int)
	for _, writeInput := range got {
		tableName := *writeInput.TableName
		for _, record := range writeInput.Records {
			measureName := *record.MeasureName
			key := tableName + "_" + measureName
			gotMetricsCount[key] += len(record.MeasureValues)
		}
	}

	assert.Equal(t, expectedMetricsCount, gotMetricsCount)

	for _, records := range got {
		for _, record := range records.Records {
			for _, mv := range record.MeasureValues {
				assert.NotNil(t, mv.Value, "The measure value should not be nil")
			}
		}
	}
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

func TestRecordsForMeasureReturnsNilWhenNoRecordsFound(t *testing.T) {
	writeRecords := timestream.WriteRecords{}
	assert.Nil(t, writeRecords.RecordsForMeasure("not_found"))
}
