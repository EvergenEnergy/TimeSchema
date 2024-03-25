package timestream_test

import (
	"fmt"
	"testing"
	"time"

	timestream "github.com/EvergenEnergy/TimeSchema/pkg"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
)

var (
	now          = time.Now()
	arrivalTime  = now.Add(1 * time.Second)
	formattedNow = fmt.Sprintf("%d", now.UnixMilli())
)

func TestMarshal(t *testing.T) {
	tests := []struct {
		name string
		args any
		want []types.Record
	}{
		{
			name: "Returns Record with multiple values",
			args: struct {
				Timestamp          time.Time `timestream:"timestamp"`
				MeasureName        string    `timestream:"measure"`
				DimensionOne       string    `timestream:"dimension,name=dimensionNameOne"`
				DimensionTwo       string    `timestream:"dimension,name=dimensionNameTwo"`
				MeasureValueString string    `timestream:"attribute,name=someMeasureValue"`
				MeasureValueFloat  float64   `timestream:"attribute,name=measureValueFloat"`
				MeasureValueInt    int       `timestream:"attribute,name=measureValueInt"`
				EmptyString        string    `timestream:"attribute,name=emptyString"`
				ArrivalTime        time.Time `timestream:"attribute,name=arrivalTime"`
				ArrivalTimeMS      time.Time `timestream:"attribute,name=arrivalTimeMs,unit=ms"`
				ArrivalTimeNS      time.Time `timestream:"attribute,name=arrivalTimeNs,unit=ns"`
				ArrivalTimeS       time.Time `timestream:"attribute,name=arrivalTimeS,unit=s"`
				OmitEmpty          string    `timestream:"attribute,name=omitEmptyColumn,omitempty"`
				IgnorableString    string
			}{
				Timestamp:          now,
				MeasureName:        "measure_name",
				DimensionOne:       "DimensionNameValueOne",
				DimensionTwo:       "DimensionNameValueTwo",
				MeasureValueString: "66",
				MeasureValueFloat:  123.00,
				MeasureValueInt:    123,
				ArrivalTime:        arrivalTime,
				ArrivalTimeMS:      arrivalTime,
				ArrivalTimeNS:      arrivalTime,
				ArrivalTimeS:       arrivalTime,
			},
			want: []types.Record{{
				Time: &formattedNow,
				Dimensions: []types.Dimension{
					{Name: aws.String("dimensionNameOne"), Value: aws.String("DimensionNameValueOne")},
					{Name: aws.String("dimensionNameTwo"), Value: aws.String("DimensionNameValueTwo")},
				},
				MeasureValues: []types.MeasureValue{
					{Name: aws.String("someMeasureValue"), Value: aws.String("66"), Type: types.MeasureValueTypeVarchar},
					{Name: aws.String("measureValueFloat"), Value: aws.String("123.000000"), Type: types.MeasureValueTypeDouble},
					{Name: aws.String("measureValueInt"), Value: aws.String("123"), Type: types.MeasureValueTypeBigint},
					{Name: aws.String("emptyString"), Value: aws.String("-"), Type: types.MeasureValueTypeVarchar},
					{Name: aws.String("arrivalTime"), Value: aws.String(fmt.Sprintf("%d", arrivalTime.Unix())), Type: types.MeasureValueTypeTimestamp},
					{Name: aws.String("arrivalTimeMs"), Value: aws.String(fmt.Sprintf("%d", arrivalTime.UnixMilli())), Type: types.MeasureValueTypeTimestamp},
					{Name: aws.String("arrivalTimeNs"), Value: aws.String(fmt.Sprintf("%d", arrivalTime.UnixNano())), Type: types.MeasureValueTypeTimestamp},
					{Name: aws.String("arrivalTimeS"), Value: aws.String(fmt.Sprintf("%d", arrivalTime.Unix())), Type: types.MeasureValueTypeTimestamp},
				},
				MeasureName: aws.String("measure_name"),
			}},
		},
		{
			name: "Returns Multiple Records with multiple values",
			args: []struct {
				Timestamp          time.Time `timestream:"timestamp"`
				MeasureName        string    `timestream:"measure"`
				DimensionOne       string    `timestream:"dimension,name=dimensionNameOne"`
				MeasureValueString string    `timestream:"attribute,name=someMeasureValue"`
			}{
				{
					Timestamp:          now,
					MeasureName:        "measure_name",
					DimensionOne:       "DimensionNameValueOne",
					MeasureValueString: "some_string",
				},
				{
					Timestamp:          now.Add(1 * time.Second),
					MeasureName:        "measure_name",
					DimensionOne:       "DimensionNameValueOne",
					MeasureValueString: "some_another_value",
				},
			},
			want: []types.Record{
				{
					Time:       &formattedNow,
					Dimensions: []types.Dimension{{Name: aws.String("dimensionNameOne"), Value: aws.String("DimensionNameValueOne")}},
					MeasureValues: []types.MeasureValue{
						{Name: aws.String("someMeasureValue"), Value: aws.String("some_string"), Type: types.MeasureValueTypeVarchar},
					},
					MeasureName: aws.String("measure_name"),
				},
				{
					Time:       aws.String(fmt.Sprintf("%d", now.Add(1*time.Second).UnixMilli())),
					Dimensions: []types.Dimension{{Name: aws.String("dimensionNameOne"), Value: aws.String("DimensionNameValueOne")}},
					MeasureValues: []types.MeasureValue{
						{Name: aws.String("someMeasureValue"), Value: aws.String("some_another_value"), Type: types.MeasureValueTypeVarchar},
					},
					MeasureName: aws.String("measure_name"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := timestream.Marshal(tt.args)
			assert.NoError(t, err)
			if diff := cmp.Diff(tt.want, got, cmpopts.IgnoreUnexported(types.Record{}, types.Dimension{}, types.MeasureValue{})); diff != "" {
				t.Errorf("Marshal() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMarshalUnhappyPath(t *testing.T) {

	tests := []struct {
		name string
		args any
	}{
		{
			name: "Returns error on primitive",
			args: 1,
		},
		{
			name: "Returns error on empty Record",
			args: struct {
				Timestamp int64 `timestream:"timestamp"`
			}{},
		},
		{
			name: "Returns err if missing timestamp",
			args: struct {
				Timestamp    time.Time `timestream:"timestamp"`
				MeasureName  string    `timestream:"measureName"`
				Dimension    string    `timestream:"dimension"`
				MeasureValue string    `timestream:"attribute"`
			}{MeasureName: "measure_name", Dimension: "dimension_name", MeasureValue: "measure_value"},
		},
		{
			name: "Returns err if using bad units",
			args: struct {
				Timestamp    time.Time `timestream:"timestamp"`
				MeasureName  string    `timestream:"measureName"`
				Dimension    string    `timestream:"dimension"`
				MeasureValue string    `timestream:"attribute"`
				BadTime      time.Time `timestream:"attribute,name=badTime,unit=bad-unit"`
			}{Timestamp: now, MeasureName: "measure_name", Dimension: "dimension_name", MeasureValue: "measure_value", BadTime: now},
		},
		{
			name: "Returns err if omitempty on non-string",
			args: struct {
				Timestamp    time.Time `timestream:"timestamp"`
				MeasureName  string    `timestream:"measureName"`
				Dimension    string    `timestream:"dimension"`
				MeasureValue float64   `timestream:"attribute,name=SomeName,omitempty"`
			}{MeasureName: "measure_name", Dimension: "dimension_name", Timestamp: now},
		},
		{
			name: "Returns err if repeated tags",
			args: struct {
				Timestamp     time.Time `timestream:"timestamp"`
				MeasureName   string    `timestream:"measureName"`
				Dimension     string    `timestream:"dimension"`
				MeasureValue  string    `timestream:"attribute,name=SomeName"`
				MeasureValue2 string    `timestream:"attribute,name=SomeName"`
			}{MeasureName: "measure_name", Dimension: "dimension_name", Timestamp: now, MeasureValue: "foo", MeasureValue2: "bar"},
		},
		{
			name: "Returns err if using a struct other than time",
			args: struct {
				Timestamp              time.Time `timestream:"timestamp"`
				MeasureName            string    `timestream:"measureName"`
				Dimension              string    `timestream:"dimension"`
				UnsupportedStructField struct {
					SomeField string
				} `timestream:"attribute,name=SomeName"`
			}{MeasureName: "measure_name", Dimension: "dimension_name", Timestamp: now, UnsupportedStructField: struct{SomeField string}{SomeField: "field"}},
		},
		{
			name: "Returns err if timestamp is not time.Time",
			args: struct {
				Timestamp    string `timestream:"timestamp"`
				MeasureName  string `timestream:"measureName"`
				Dimension    string `timestream:"dimension"`
				MeasureValue string `timestream:"attribute"`
			}{
				Timestamp:    "not_a_time",
				MeasureName:  "measure_name",
				Dimension:    "dimension_name",
				MeasureValue: "measure_value",
			},
		},
		{
			name: "Returns err if multiple timestamp fields",
			args: struct {
				Timestamp    time.Time `timestream:"timestamp"`
				SecondTS     time.Time `timestream:"timestamp"`
				MeasureName  string    `timestream:"measureName"`
				Dimension    string    `timestream:"dimension"`
				MeasureValue string    `timestream:"attribute"`
			}{
				Timestamp:    now,
				SecondTS:     now,
				MeasureName:  "measure_name",
				Dimension:    "dimension_name",
				MeasureValue: "measure_value",
			},
		},
		{
			name: "Returns err if missing a measure name value",
			args: struct {
				Timestamp    time.Time `timestream:"timestamp"`
				MeasureName  string    `timestream:"measureName"`
				Dimension    string    `timestream:"dimension"`
				MeasureValue string    `timestream:"attribute"`
			}{Timestamp: now, Dimension: "dimension_name", MeasureValue: "measure_value"},
		},
		{
			name: "Returns err if multiple measure names",
			args: struct {
				Timestamp    time.Time `timestream:"timestamp"`
				MeasureName  string    `timestream:"measureName"`
				AnotherName  string    `timestream:"measureName"`
				Dimension    string    `timestream:"dimension"`
				MeasureValue string    `timestream:"attribute"`
			}{
				Timestamp:    now,
				Dimension:    "dimension_name",
				MeasureValue: "measure_value",
				MeasureName:  "measure_name",
				AnotherName:  "another_name",
			},
		},
		{
			name: "Fails if attribute is anything but a primitive",
			args: struct {
				Timestamp time.Time `timestream:"timestamp"`
				Measure   string    `timestream:"measure"`
				Dimension string    `timestream:"dimension,name=dimensionNameOne"`
				Attribute struct {
					NestedField string
				} `timestream:"attribute,name=someMeasureValue"`
			}{
				Timestamp: now,
				Measure:   "measure_name",
				Attribute: struct{ NestedField string }{NestedField: "nested"},
				Dimension: "DimensionNameValueOne",
			},
		},
		{
			name: "Fails if values aren't exported",
			args: struct {
				timestamp    time.Time `timestream:"timestamp"`
				measureName  string    `timestream:"measure"`
				Dimension    string    `timestream:"dimension,name=dimensionNameOne"`
				MeasureValue string    `timestream:"attribute,name=someMeasureValue"`
			}{
				timestamp:    now,
				measureName:  "measure_name",
				MeasureValue: "66",
				Dimension:    "DimensionNameValueOne",
			},
		},
		{
			name: "Fails if one value in the collection is invalid",
			args: []struct {
				Timestamp    time.Time `timestream:"timestamp"`
				MeasureName  string    `timestream:"measure"`
				Dimension    string    `timestream:"dimension,name=dimensionNameOne"`
				MeasureValue string    `timestream:"attribute,name=someMeasureValue"`
			}{
				{
					Timestamp:    now,
					MeasureName:  "measure_name",
					MeasureValue: "66",
					Dimension:    "DimensionNameValueOne",
				},
				{
					MeasureName:  "measure_name",
					MeasureValue: "66",
					Dimension:    "DimensionNameValueOne",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := timestream.Marshal(tt.args)
			assert.Error(t, err)
			assert.Nil(t, res)
		})
	}
}
