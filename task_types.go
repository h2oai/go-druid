package druid

// TaskStatusResponse is a response object containing status of a task.
type TaskStatusResponse struct {
	Task   string     `json:"task"`
	Status TaskStatus `json:"status"`
}

// TaskLocation holds location of the task execution.
type TaskLocation struct {
	Host    string `json:"host"`
	Port    int    `json:"port"`
	TlsPort int    `json:"tlsPort"`
}

// TaskStatus is an object representing status of a druid task.
type TaskStatus struct {
	ID                 string        `json:"id"`
	Type               string        `json:"type"`
	CreatedTime        string        `json:"createdTime"`
	QueueInsertionTime string        `json:"queueInsertionTime"`
	StatusCode         string        `json:"statusCode"`
	Status             string        `json:"status"`
	RunnerStatusCode   string        `json:"runnerStatusCode"`
	Duration           int           `json:"duration"`
	GroupId            string        `json:"groupId"`
	Location           *TaskLocation `json:"location|omitempty"`
	Datasource         string        `json:"datasource"`
	ErrorMessage       string        `json:"errorMessage"`
}

// TaskIngestionSpec is a specification for a druid task execution.
type TaskIngestionSpec struct {
	Type string             `json:"type"`
	Spec *IngestionSpecData `json:"spec"`
}

// defaultTaskIngestionSpec returns a default TaskIngestionSpec with basic ingestion
// specification fields initialized.
func defaultTaskIngestionSpec() *TaskIngestionSpec {
	spec := &TaskIngestionSpec{
		Type: "index_parallel",
		Spec: &IngestionSpecData{
			DataSchema: &DataSchema{
				DataSource: "some_datasource",
				TimeStampSpec: &TimestampSpec{
					Column: "ts",
					Format: "auto",
				},
				GranularitySpec: &GranularitySpec{
					Type:               "uniform",
					SegmentGranularity: "DAY",
					QueryGranularity:   "none",
				},
				DimensionsSpec: &DimensionsSpec{
					UseSchemaDiscovery: true,
					Dimensions:         DimensionSet{},
				},
				TransformSpec: &TransformSpec{
					Transforms: []Transform{},
				},
			},
			IOConfig: &IOConfig{
				Type: "index_parallel",
				InputSource: &InputSource{
					Type: "sql",
					Database: &Database{
						Type: "postgresql",
						ConnectorConfig: &ConnectorConfig{
							ConnectURI: "jdbc:postgresql://host:port/schema",
							User:       "user",
							Password:   "password",
						},
					},
					SQLs: []string{},
				},
				InputFormat: &InputFormat{
					Type: "json",
				},
			},
			TuningConfig: &TuningConfig{
				Type: "index_parallel",
			},
		},
	}
	return spec
}

// TaskIngestionSpecOptions allows for configuring a TaskIngestionSpec.
type TaskIngestionSpecOptions func(*TaskIngestionSpec)

// SetTaskType sets the type of the task IOConfig.
func SetTaskType(stype string) TaskIngestionSpecOptions {
	return func(spec *TaskIngestionSpec) {
		if stype != "" {
			spec.Type = stype
		}
	}
}

// SetTaskDataSource sets the destination datasource of the task IOConfig.
func SetTaskDataSource(datasource string) TaskIngestionSpecOptions {
	return func(spec *TaskIngestionSpec) {
		if datasource != "" {
			spec.Spec.DataSchema.DataSource = datasource
		}
	}
}

// SetTaskTuningConfig sets the tuning configuration the task IOConfig.
func SetTaskTuningConfig(typ string, maxRowsInMemory, maxRowsPerSegment int) TaskIngestionSpecOptions {
	return func(spec *TaskIngestionSpec) {
		if typ != "" {
			spec.Spec.TuningConfig.Type = typ
			spec.Spec.TuningConfig.MaxRowsInMemory = maxRowsInMemory
			spec.Spec.TuningConfig.MaxRowsPerSegment = maxRowsPerSegment
		}
	}
}

// SetTaskDataDimensions sets druid datasource dimensions.
func SetTaskDataDimensions(dimensions DimensionSet) TaskIngestionSpecOptions {
	return func(spec *TaskIngestionSpec) {
		spec.Spec.DataSchema.DimensionsSpec.Dimensions = dimensions
	}
}

// SetTaskSQLInputSource configures sql input source for the task based ingestion.
func SetTaskSQLInputSource(typ, connectURI, user, password string, sqls []string) TaskIngestionSpecOptions {
	return func(spec *TaskIngestionSpec) {
		spec.Spec.IOConfig.InputSource = &InputSource{
			Type: "sql",
			SQLs: sqls,
			Database: &Database{
				Type: typ,
				ConnectorConfig: &ConnectorConfig{
					ConnectURI: connectURI,
					User:       user,
					Password:   password,
				},
			},
		}
	}
}

// SetTaskIOConfigType sets the type of the task IOConfig.
func SetTaskIOConfigType(typ string) TaskIngestionSpecOptions {
	return func(spec *TaskIngestionSpec) {
		if typ != "" {
			spec.Spec.IOConfig.Type = typ
		}
	}
}

// SetTaskInputFormat configures input format for the task based ingestion.
func SetTaskInputFormat(typ string, findColumnsHeader string, columns []string) TaskIngestionSpecOptions {
	return func(spec *TaskIngestionSpec) {
		spec.Spec.IOConfig.InputFormat.Type = typ
		spec.Spec.IOConfig.InputFormat.FindColumnsHeader = findColumnsHeader
		spec.Spec.IOConfig.InputFormat.Columns = columns
	}
}

// SetTaskInlineInputData configures inline data for the task based ingestion.
func SetTaskInlineInputData(data string) TaskIngestionSpecOptions {
	return func(spec *TaskIngestionSpec) {
		spec.Spec.IOConfig.InputSource.Type = "inline"
		spec.Spec.IOConfig.InputSource.Data = data
	}
}

// NewTaskIngestionSpec returns a default TaskIngestionSpec and applies any options passed to it.
func NewTaskIngestionSpec(options ...TaskIngestionSpecOptions) *TaskIngestionSpec {
	spec := defaultTaskIngestionSpec()
	for _, fn := range options {
		fn(spec)
	}
	return spec
}
