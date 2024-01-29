package timestream_test

import (
	"math"
	"testing"
	"time"

	timestream "github.com/EvergenEnergy/TimeSchema/pkg"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
)

func TestUnmarshal(t *testing.T) {
	type MyData struct {
		Timestamp   time.Time `timestream:"time"`
		Name        string    `timestream:"name=dimension_name"`
		Energy      float64   `timestream:"name=modelled_generation"`
		Power       int       `timestream:"name=actual_pv_power"`
		ArrivalTime time.Time `timestream:"name=received_time"`
		Ignorable   string
		Unused      string `timestream:"-"`
	}

	tests := []struct {
		name   string
		record *timestreamquery.QueryOutput
		target any
		want   any
	}{
		{
			name: "Successfully handles response with no records",
			record: &timestreamquery.QueryOutput{
				ColumnInfo: []types.ColumnInfo{
					{Type: &types.Type{ScalarType: types.ScalarTypeTimestamp}, Name: aws.String("time")},
					{Type: &types.Type{ScalarType: types.ScalarTypeVarchar}, Name: aws.String("dimension_name")},
				},
				Rows:    []types.Row{},
				QueryId: aws.String("AEHQCANRQXMATV22GTB2SD4PTDZISJMXF2CBU767QOYCDD2KPCUNRT2IB4REZAI"),
			},
			target: &MyData{},
			want:   &MyData{},
		},
		{
			name: "Successfully unmarshals into single struct",
			record: &timestreamquery.QueryOutput{
				ColumnInfo: []types.ColumnInfo{
					{Type: &types.Type{ScalarType: types.ScalarTypeTimestamp}, Name: aws.String("time")},
					{Type: &types.Type{ScalarType: types.ScalarTypeVarchar}, Name: aws.String("dimension_name")},
					{Type: &types.Type{ScalarType: types.ScalarTypeDouble}, Name: aws.String("modelled_generation")},
					{Type: &types.Type{ScalarType: types.ScalarTypeInteger}, Name: aws.String("actual_pv_power")},
					{Type: &types.Type{ScalarType: types.ScalarTypeTimestamp}, Name: aws.String("received_time")},
				},
				Rows: []types.Row{{Data: []types.Datum{
					{ScalarValue: aws.String("2024-01-08 02:32:04.000000000")},
					{ScalarValue: aws.String("A dimension name")},
					{ScalarValue: aws.String("10.5")},
					{ScalarValue: aws.String("10")},
					{ScalarValue: aws.String("2024-01-29 02:55:00.000000000")},
				}}},
				QueryId: aws.String("AEHQCANRQXMATV22GTB2SD4PTDZISJMXF2CBU767QOYCDD2KPCUNRT2IB4REZAI"),
			},
			target: &MyData{},
			want: &MyData{
				Timestamp:   time.Date(2024, time.January, 8, 2, 32, 4, 0, time.UTC),
				Name:        "A dimension name",
				Energy:      10.5,
				Power:       10,
				ArrivalTime: time.Date(2024, time.January, 29, 2, 55, 0, 0, time.UTC),
			},
		},
		{
			name: "Successfully unmarshals into slice",
			record: &timestreamquery.QueryOutput{
				ColumnInfo: []types.ColumnInfo{
					{Type: &types.Type{ScalarType: types.ScalarTypeTimestamp}, Name: aws.String("time")},
					{Type: &types.Type{ScalarType: types.ScalarTypeVarchar}, Name: aws.String("dimension_name")},
					{Type: &types.Type{ScalarType: types.ScalarTypeDouble}, Name: aws.String("modelled_generation")},
					{Type: &types.Type{ScalarType: types.ScalarTypeInteger}, Name: aws.String("actual_pv_power")},
					{Type: &types.Type{ScalarType: types.ScalarTypeTimestamp}, Name: aws.String("received_time")},
				},
				Rows: []types.Row{{Data: []types.Datum{
					{ScalarValue: aws.String("2024-01-08 02:32:04.000000000")},
					{ScalarValue: aws.String("A dimension name")},
					{ScalarValue: aws.String("10.5")},
					{ScalarValue: aws.String("10")},
					{ScalarValue: aws.String("2024-01-29 02:55:00.000000000")},
				}}, {Data: []types.Datum{
					{ScalarValue: aws.String("2024-01-08 02:33:04.000000000")},
					{ScalarValue: aws.String("A dimension name")},
					{ScalarValue: aws.String("11.5")},
					{ScalarValue: aws.String("11")},
					{ScalarValue: aws.String("2024-01-29 02:55:05.000000000")},
				}}},
				QueryId: aws.String("AEHQCANRQXMATV22GTB2SD4PTDZISJMXF2CBU767QOYCDD2KPCUNRT2IB4REZAI"),
			},
			target: &[]MyData{},
			want: &[]MyData{{
				Timestamp:   time.Date(2024, time.January, 8, 2, 32, 4, 0, time.UTC),
				Name:        "A dimension name",
				Energy:      10.5,
				Power:       10,
				ArrivalTime: time.Date(2024, time.January, 29, 2, 55, 0, 0, time.UTC),
			}, {
				Timestamp:   time.Date(2024, time.January, 8, 2, 33, 4, 0, time.UTC),
				Name:        "A dimension name",
				Energy:      11.5,
				Power:       11,
				ArrivalTime: time.Date(2024, time.January, 29, 2, 55, 5, 0, time.UTC),
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := timestream.Unmarshal(tt.record, tt.target)
			assert.NoError(t, err)
			cmpOpts := cmp.Options{
				cmp.Comparer(func(x, y time.Time) bool {
					return math.Abs(float64(x.Sub(y))) < float64(time.Millisecond)
				}),
				cmpopts.IgnoreUnexported(MyData{}),
			}
			// If no error expected, compare the struct's actual state to the expected state
			if diff := cmp.Diff(tt.want, tt.target, cmpOpts, cmpopts.IgnoreUnexported(timestreamquery.QueryOutput{})); diff != "" {
				t.Errorf("Unmarshal() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestUnmarshalUnhappyPath(t *testing.T) {
	tests := []struct {
		name   string
		record *timestreamquery.QueryOutput
		target any
	}{
		{
			name:   "Returns error on primitive",
			record: &timestreamquery.QueryOutput{},
			target: 1,
		},
		{
			name:   "Returns error on nil Record",
			record: nil,
			target: &struct {
				Timestamp int64 `timestream:"timestamp"`
			}{Timestamp: 1234567890},
		},
		{
			name: "Returns error on pointer to primitive",
			record: &timestreamquery.QueryOutput{
				ColumnInfo: []types.ColumnInfo{
					{Type: &types.Type{ScalarType: types.ScalarTypeTimestamp}, Name: aws.String("time")},
				},
				Rows: []types.Row{{Data: []types.Datum{
					{ScalarValue: aws.String("2024-01-08 02:32:04.000000000")},
				}}},
				QueryId: aws.String("AEHQCANRQXMATV22GTB2SD4PTDZISJMXF2CBU767QOYCDD2KPCUNRT2IB4REZAI"),
			},
			target: aws.String("some pointer to string"),
		},
		{
			name: "Returns error for bad tags",
			record: &timestreamquery.QueryOutput{
				ColumnInfo: []types.ColumnInfo{
					{Type: &types.Type{ScalarType: types.ScalarTypeTimestamp}, Name: aws.String("time")},
					{Type: &types.Type{ScalarType: types.ScalarTypeTimestamp}, Name: aws.String("bad=tag")},
				},
				Rows: []types.Row{{Data: []types.Datum{
					{ScalarValue: aws.String("2024-01-08 02:32:04.000000000")},
					{ScalarValue: aws.String("bad data")},
				}}},
				QueryId: aws.String("AEHQCANRQXMATV22GTB2SD4PTDZISJMXF2CBU767QOYCDD2KPCUNRT2IB4REZAI"),
			},
			target: &struct {
				Timestamp string `timestream:"time"`
				BadTag    string `timestream:"bad=tag"`
			}{Timestamp: "1234567890", BadTag: "some string"},
		},
		{
			name: "Returns error when providing a single struct and a query result with multiple rows",
			record: &timestreamquery.QueryOutput{
				ColumnInfo: []types.ColumnInfo{
					{Type: &types.Type{ScalarType: types.ScalarTypeTimestamp}, Name: aws.String("time")},
				},
				Rows: []types.Row{{Data: []types.Datum{
					{ScalarValue: aws.String("2024-01-08 02:32:04.000000000")},
				}}, {Data: []types.Datum{
					{ScalarValue: aws.String("2024-01-08 02:33:04.000000000")},
				}}},
				QueryId: aws.String("AEHQCANRQXMATV22GTB2SD4PTDZISJMXF2CBU767QOYCDD2KPCUNRT2IB4REZAI"),
			},
			target: &struct {
				Timestamp int64 `timestream:"timestamp"`
			}{},
		},
		{
			name: "Returns error when target has a different length than the record",
			record: &timestreamquery.QueryOutput{
				ColumnInfo: []types.ColumnInfo{
					{Type: &types.Type{ScalarType: types.ScalarTypeTimestamp}, Name: aws.String("time")},
				},
				Rows: []types.Row{{Data: []types.Datum{
					{ScalarValue: aws.String("2024-01-08 02:32:04.000000000")},
				}}},
				QueryId: aws.String("AEHQCANRQXMATV22GTB2SD4PTDZISJMXF2CBU767QOYCDD2KPCUNRT2IB4REZAI"),
			},
			target: &[]struct {
				Timestamp int64 `timestream:"timestamp"`
			}{{Timestamp: 1234567890}, {Timestamp: 1234567891}},
		},
		{
			name: "Returns error when target has incompatible data type",
			record: &timestreamquery.QueryOutput{
				ColumnInfo: []types.ColumnInfo{
					{Type: &types.Type{ScalarType: types.ScalarTypeTimestamp}, Name: aws.String("time")},
				},
				Rows: []types.Row{{Data: []types.Datum{
					{ScalarValue: aws.String("2024-01-08 02:32:04.000000000")},
				}}},
				QueryId: aws.String("AEHQCANRQXMATV22GTB2SD4PTDZISJMXF2CBU767QOYCDD2KPCUNRT2IB4REZAI"),
			},
			target: &struct {
				Timestamp int `timestream:"time"`
			}{Timestamp: 1234567890},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := timestream.Unmarshal(tt.record, tt.target)
			assert.Error(t, err)
		})
	}
}
