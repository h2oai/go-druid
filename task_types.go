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

// defaultKafkaIngestionSpec returns a default InputIngestionSpec with basic ingestion
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
					Dimensions: DimensionSet{},
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
					SQLs: []string{"SELECT * FROM some_table"},
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

// IngestionSpecOptions allows for configuring a InputIngestionSpec.
type TaskIngestionSpecOptions func(*TaskIngestionSpec)

// SetType sets the type of the supervisor (IOConfig).
func SetTaskType(stype string) TaskIngestionSpecOptions {
	return func(spec *TaskIngestionSpec) {
		if stype != "" {
			spec.Type = stype
		}
	}
}

// SetType sets the type of the supervisor (IOConfig).
func SetTaskDataSource(datasource string) TaskIngestionSpecOptions {
	return func(spec *TaskIngestionSpec) {
		if datasource != "" {
			spec.Spec.DataSchema.DataSource = datasource
		}
	}
}

// SetTuningConfig sets the type of the supervisor (IOConfig).
func SetTuningConfig(ttype string, maxRowsInMemory, maxRowsPerSegment int) TaskIngestionSpecOptions {
	return func(spec *TaskIngestionSpec) {
		if ttype != "" {
			spec.Spec.TuningConfig.Type = ttype
			spec.Spec.TuningConfig.MaxRowsInMemory = maxRowsInMemory
			spec.Spec.TuningConfig.MaxRowsPerSegment = maxRowsPerSegment
		}
	}
}

// NewTaskIngestionSpec returns a default TaskIngestionSpec and applies any
// options passed to it.
func NewTaskIngestionSpec(options ...TaskIngestionSpecOptions) *TaskIngestionSpec {
	spec := defaultTaskIngestionSpec()
	for _, fn := range options {
		fn(spec)
	}
	return spec
}
