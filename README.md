# TimeSchema: A Comprehensive AWS Timestream Go Library

TimeSchema is a Go library tailored for interacting with AWS Timestream, offering functionalities for marshalling and unmarshalling data, dynamic query building, and effective schema management.

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Coverage Status](https://coveralls.io/repos/github/EvergenEnergy/TimeSchema/badge.svg?branch=main)](https://coveralls.io/github/EvergenEnergy/TimeSchema?branch=main)

## Features
- **Data Marshalling**: Convert Go structs to AWS Timestream records with ease, using struct tags for precise field mapping.
- Supports specifying time units (seconds, milliseconds, nanoseconds) for `time.Time` fields in structs.
- **Data Unmarshalling**: Seamlessly decode AWS Timestream query outputs into Go structs or slices of structs.
- **Query Building**: Dynamically construct SQL queries for Timestream with named placeholders and a variety of data types.
- **Schema Management**: Utilize generic types for flexible and efficient schema definitions in AWS Timestream.

## Usage

### Marshalling
Convert Go structs into AWS Timestream `types.Record` for easy data insertion.

**Example:**
```go
package main

import (
    "time"
    "github.com/EvergenEnergy/TimeSchema"
)

type MyData struct {
    Time         time.Time `timestream:"timestamp"`
    SensorName   string    `timestream:"measure"`
    Location     string    `timestream:"dimension,name=location"`
    Temperature  float64   `timestream:"attribute,name=temperature"`
    EventTime    time.Time `timestream:"attribute,name=eventTime,unit=ms"`
}

func main() {
    data := MyData{
        Time:         time.Now(),
        SensorName:   "Sensor1",
        Location:     "Room1",
        Temperature:  23.5,
        EventTime:    time.Now(),
    }

    record, err := timeschema.Marshal(data)
    if err != nil {
        // handle error
    }
    // Use record with AWS Timestream
}
```

### Unmarshalling
Decode AWS Timestream query output into your Go data structures.

**Example:**
```go
package main

import (
    "github.com/EvergenEnergy/TimeSchema"
    "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
)

type MyData struct {
    Timestamp   time.Time `timestream:"time"`
    Name        string    `timestream:"name=dimension_name"`
    Energy      float64   `timestream:"name=modelled_generation"`
    Power       int       `timestream:"name=actual_pv_power"`
}

func main() {
    var myData MyData
    var queryOutput *timestreamquery.QueryOutput // Assume this is obtained from Timestream query

    err := timeschema.Unmarshal(queryOutput, &myData)
    if err != nil {
        // handle error
    }

    var myDataSlice []MyData
    err = timeschema.Unmarshal(queryOutput, &myDataSlice)
    if err != nil {
        // handle error
    }
}
```

### Query Building
Create SQL queries with parameterized inputs for enhanced security and flexibility.

**Example:**
The `BuildQuery` function allows you to create SQL queries by replacing placeholders with actual values from a parameters map. Supported types include string, time.Time, int, int64, float64, and custom types like `DatabaseName` and `TableName`.

**Usage:**
```go
import (
"github.com/EvergenEnergy/TimeSchema"
"time"
)

func main() {
    template := "SELECT * FROM :tableName WHERE name = :name AND timestamp = :timestamp AND id = :id"
    params := map[string]interface{}{
        "name":      "test",
        "timestamp": time.Now(),
        "id":        123,
        "tableName": timeschema.TableName("my_table"),
    }
    
    query, err := timeschema.BuildQuery(template, params)
    if err != nil {
    // handle error
    }
    
    // Use query with AWS Timestream
}
```

## Enhanced Schema Management with Dimensions and Dummy Data Generation

TimeSchema now supports an advanced schema definition that includes dimensions alongside metric names, enabling more comprehensive data modeling for AWS Timestream. Additionally, the library offers functionality to generate dummy data based on the defined schema, facilitating testing and development with realistic data scenarios.

### Defining a Schema with Dimensions

Define your Timestream schema using generic types for flexibility. This allows for defining dimensions and metrics within your schema, providing a structured approach to data representation.

**Example:**
```go
import (
timestream "github.com/EvergenEnergy/TimeSchema"
)

// Define your schema
schema := timestream.Schema[string, string]{
    "YourTableName": {
        "YourMeasureName": {
            Dimensions:  []string{"Dimension1", "Dimension2"},
            MetricNames: []string{"Metric1", "Metric2"},
        },
    },
}

// Initialize a new TSSchema instance with the defined schema
tsSchema := timestream.NewTSSchema(schema)
```
This schema definition allows you to clearly specify which dimensions and metrics are associated with each measure within a table, enhancing the clarity and maintainability of your Timestream data models.

### Generating Dummy Data

Easily generate dummy data for testing or development purposes based on your schema. This feature supports predefined values for metrics, or randomly generated data where no predefined values are specified.

**Example:**
```go
import (
    "time"
    timestream "github.com/EvergenEnergy/TimeSchema"
)

// Assuming tsSchema is your TSSchema instance and schema is defined as above
predefinedValues := timestream.PredefinedValues[string]{
    "Metric1": 100,
    "Metric2": 200,
    // Add more predefined metrics if necessary
}

// Generate dummy data
now := time.Now() // Specify the current time or any timestamp you need
dbName := "YourDatabaseName"
dummyData := tsSchema.GenerateDummyData(dbName, now, predefinedValues)

// dummyData is now populated with WriteRecordsInput instances that can be used with AWS Timestream

```

The `GenerateDummyData` method allows for the creation of data entries that match the structure of your defined schema, making it an invaluable tool for simulating real-world data ingestion and processing workflows.

### Installation

To use TimeSchema, install the package using go get:

```bash
go get github.com/EvergenEnergy/TimeSchema
```

### Contributing

Contributions are welcome! Feel free to submit pull requests, open issues, or suggest new features.

### License

TimeSchema is distributed under the Apache License, Version 2.0. See the `LICENSE` file in the GitHub repository for more details.
