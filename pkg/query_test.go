package timestream_test

import (
	"testing"
	"time"

	timestream "github.com/EvergenEnergy/TimeSchema/pkg"
	"github.com/stretchr/testify/assert"
)

var fixedNow = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

func TestBuildQueryFails(t *testing.T) {
	template := "SELECT * FROM my_table WHERE name = :name AND timestamp = :timestamp AND id = :id"
	params := map[string]interface{}{
		"bad_placeholder": "test",
	}

	result, err := timestream.BuildQuery(template, params)
	assert.Error(t, err)
	assert.Equal(t, "", result)
}

func TestBuildQueryFailsWithStruct(t *testing.T) {
	template := "SELECT * FROM my_table WHERE name = :name AND timestamp = :timestamp AND id = :id"
	params := map[string]interface{}{
		"name":      "test",
		"timestamp": time.Now(),
		"id":        struct{ some string }{some: "value"},
	}

	result, err := timestream.BuildQuery(template, params)
	assert.Error(t, err)
	assert.Equal(t, "", result)
}

func TestBuildQuery(t *testing.T) {
	type args struct {
		template string
		params   map[string]interface{}
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "simple query",
			args: args{
				template: "SELECT * FROM my_table WHERE name = :name AND timestamp = :timestamp AND id = :id",
				params:   map[string]interface{}{"name": "test", "timestamp": fixedNow, "id": 1},
			},
			want: "SELECT * FROM my_table WHERE name = 'test' AND timestamp = from_unixtime(1704067200) AND id = 1",
		},
		{
			name: "test time",
			args: args{
				template: "SELECT * FROM my_table WHERE name = :name AND timestamp BETWEEN :yesterday AND :now",
				params:   map[string]interface{}{"name": "test", "yesterday": fixedNow.Add(-24 * time.Hour), "now": fixedNow},
			},
			want: "SELECT * FROM my_table WHERE name = 'test' AND timestamp BETWEEN from_unixtime(1703980800) AND from_unixtime(1704067200)",
		},
		{
			name: "test duration",
			args: args{
				template: "SELECT * FROM my_table WHERE name = :name AND timestamp BETWEEN ago(:yesterday) AND ago(:now)",
				params:   map[string]interface{}{"name": "test", "yesterday": (24 * time.Hour), "now": 1 * time.Second},
			},
			want: "SELECT * FROM my_table WHERE name = 'test' AND timestamp BETWEEN ago(86400s) AND ago(1s)",
		},
		{
			name: "test table name",
			args: args{
				template: "SELECT * FROM :tableName WHERE name = :name AND id = :id",
				params:   map[string]interface{}{"name": "test", "id": 1, "tableName": timestream.TableName("my_table")},
			},
			want: "SELECT * FROM \"my_table\" WHERE name = 'test' AND id = 1",
		},
		{
			name: "test database name",
			args: args{
				template: "SELECT * FROM :database.:tableName",
				params: map[string]interface{}{
					"tableName": timestream.TableName("my_table"),
					"database":  timestream.DatabaseName("my_database"),
				},
			},
			want: "SELECT * FROM \"my_database\".\"my_table\"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := timestream.BuildQuery(tt.args.template, tt.args.params)
			assert.NoError(t, err)
			assert.Equalf(t, tt.want, got, "BuildQuery(%v, %v)", tt.args.template, tt.args.params)
		})
	}
}
