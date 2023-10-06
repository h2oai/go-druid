package druid

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaIngestionSpec(t *testing.T) {
	var testData = []struct {
		name     string
		options  []IngestionSpecOptions
		expected *InputIngestionSpec
	}{
		{
			name: "set datasource, topic, brokers and duration",
			options: []IngestionSpecOptions{
				SetTopic("test_topic"),
				SetDataSource("test_source"),
				SetTaskDuration("PT20S"),
				SetBrokers("test_brokers"),
			},
			expected: func() *InputIngestionSpec {
				out := defaultKafkaIngestionSpec()
				out.IOConfig.Topic = "test_topic"
				out.IOConfig.ConsumerProperties.BootstrapServers = "test_brokers"
				out.DataSchema.DataSource = "test_source"
				out.IOConfig.TaskDuration = "PT20S"
				return out
			}(),
		},
		{
			name: "set labels",
			options: []IngestionSpecOptions{
				SetDimensions([]interface{}{"ts", "user_name", "payload"}),
			},
			expected: func() *InputIngestionSpec {
				out := defaultKafkaIngestionSpec()
				out.DataSchema.DimensionsSpec.Dimensions = []interface{}{"ts", "user_name", "payload"}
				return out
			}(),
		},
	}

	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			actual := NewIngestionSpec(
				test.options...,
			)
			assert.Equal(t, test.expected, actual)
		})
	}
}

var jsonBasic = `{
    "type": "kafka",
    "dataSchema": {
        "dataSource": "test_datasource",
        "timestampSpec": {
            "column": "ts",
            "format": "auto"
        },
        "transformSpec": {
            "transforms": []
        },
        "dimensionsSpec": {
            "dimensions": [
                "ts",
                "user_name",
                "payload"
            ]
        },
        "granularitySpec": {
            "type": "uniform",
            "segmentGranularity": "DAY",
            "queryGranularity": "none"
        }
    },
    "ioConfig": {
        "topic": "test_topic",
        "consumerProperties": {
            "bootstrap.servers": "test_brokers"
        },
        "taskDuration": "PT1H",
        "useEarliestOffset": false,
        "flattenSpec": {
            "fields": []
        },
        "inputFormat": {
            "type": "json"
        }
    }
}`

func TestKafkaIngestionSpec_MarshalJSON(t *testing.T) {
	t.Run("jsonBasic", func(t *testing.T) {
		spec := NewIngestionSpec(
			SetDataSource("test_datasource"),
			SetTopic("test_topic"),
			SetBrokers("test_brokers"),
			SetDimensions([]interface{}{"ts", "user_name", "payload"}),
		)
		actual, err := json.MarshalIndent(spec, "", "    ")
		if err != nil {
			t.Fatalf("unexpected error while marshalling: %v", err)
		}
		expected := []byte(jsonBasic)
		require.Equal(t, string(expected), string(actual), fmt.Sprintf("expected: %s\nactual: %s", string(expected), string(actual)))

		var checkSpec *InputIngestionSpec
		err = json.Unmarshal(actual, &checkSpec)
		if err != nil {
			t.Fatalf("unexpected error while unmarshalling: %v", err)
		}
		require.Equal(t, spec, checkSpec)
	})
}

var jsonWithTypedDimensions = `{
    "type": "kafka",
    "dataSchema": {
        "dataSource": "test_datasource",
        "timestampSpec": {
            "column": "ts",
            "format": "auto"
        },
        "transformSpec": {
            "transforms": []
        },
        "dimensionsSpec": {
            "dimensions": [
                {
                    "type": "string",
                    "name": "ts"
                },
                {
                    "type": "json",
                    "name": "payload"
                }
            ]
        },
        "granularitySpec": {
            "type": "uniform",
            "segmentGranularity": "DAY",
            "queryGranularity": "none"
        }
    },
    "ioConfig": {
        "topic": "test_topic",
        "consumerProperties": {
            "bootstrap.servers": "test_brokers"
        },
        "taskDuration": "PT1H",
        "useEarliestOffset": false,
        "flattenSpec": {
            "fields": []
        },
        "inputFormat": {
            "type": "json"
        }
    }
}`

func TestIngestionSpecWithTypedDimensions_MarshalJSON(t *testing.T) {
	t.Run("jsonWithTypedDimensions", func(t *testing.T) {
		spec := NewIngestionSpec(
			SetDataSource("test_datasource"),
			SetTopic("test_topic"),
			SetBrokers("test_brokers"),
			SetDimensions([]interface{}{
				Dimension{Type: "string", Name: "ts"},
				Dimension{Type: "json", Name: "payload"},
			}),
		)
		actual, err := json.MarshalIndent(spec, "", "    ")
		if err != nil {
			t.Fatalf("unexpected error while marshalling: %v", err)
		}
		expected := []byte(jsonWithTypedDimensions)
		require.Equal(t, string(expected), string(actual), fmt.Sprintf("expected: %s\nactual: %s", string(expected), string(actual)))
	})
}
