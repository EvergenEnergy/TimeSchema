package timestream_test

import (
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
			name:   "Test gets correct measure name",
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
