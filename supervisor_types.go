package druid

import "time"

// InputIngestionSpec is the root-level type defining an ingestion spec used
// by Apache Druid.
type InputIngestionSpec struct {
	Type         string        `json:"type"`
	DataSchema   *DataSchema   `json:"dataSchema,omitempty"`
	IOConfig     *IOConfig     `json:"ioConfig,omitempty"`
	TuningConfig *TuningConfig `json:"tuningConfig,omitempty"`
}

type IngestionSpecData struct {
	DataSchema   *DataSchema   `json:"dataSchema,omitempty"`
	IOConfig     *IOConfig     `json:"ioConfig,omitempty"`
	TuningConfig *TuningConfig `json:"tuningConfig,omitempty"`
}

type OutputIngestionSpec struct {
	Type      string             `json:"type"`
	Context   string             `json:"context"`
	Suspended bool               `json:"suspended"`
	Spec      *IngestionSpecData `json:"spec"`
}

type TuningConfig struct {
	Type                      string `json:"type"`
	IntermediatePersistPeriod string `json:"intermediatePersistPeriod,omitempty"`
	MaxRowsPerSegment         int    `json:"maxRowsPerSegment,omitempty"`
	MaxRowsInMemory           int    `json:"maxRowsInMemory,omitempty"`
}

// Metric is a Druid aggregator that is applied at ingestion time.
type Metric struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	FieldName string `json:"fieldName"`
}

// DataSchema represents the Druid dataSchema spec.
type DataSchema struct {
	DataSource      string           `json:"dataSource"`
	Parser          string           `json:"parser,omitempty"`
	TimeStampSpec   *TimestampSpec   `json:"timestampSpec,omitempty"`
	TransformSpec   *TransformSpec   `json:"transformSpec,omitempty"`
	DimensionsSpec  *DimensionsSpec  `json:"dimensionsSpec,omitempty"`
	GranularitySpec *GranularitySpec `json:"granularitySpec,omitempty"`
	MetricSpec      []Metric         `json:"metricSpec,omitempty"`
}

// FlattenSpec is responsible for flattening nested input
// JSON data into Druid's flat data model.
type FlattenSpec struct {
	Fields FieldList `json:"fields"`
}

// TimestampSpec is responsible for configuring the primary timestamp.
type TimestampSpec struct {
	Column string `json:"column"`
	Format string `json:"format"`
}

// FieldList is a list of Fields for ingestion FlattenSpec.
type FieldList []Field

// Field defines a single filed configuration of the FlattenSpec.
type Field struct {
	Type string `json:"type"`
	Name string `json:"name"`
	Expr string `json:"expression"`
}

// Transform defines a single filed transformation of the TransformSpec.
type Transform struct {
	Type string `json:"type"`
	Name string `json:"name"`
	Expr string `json:"expression"`
}

// TransformSet is a unique set of transforms applied to the input.
type TransformSet []Transform

// DimensionSet is a unique set of druid datasource dimensions(labels).
type DimensionSet []string

// DimensionsSpec is responsible for configuring Druid's dimensions. They're a
// set of columns in Druid's data model that can be used for grouping, filtering
// or applying aggregations.
type DimensionsSpec struct {
	Dimensions DimensionSet `json:"dimensions"`
}

// GranularitySpec allows for configuring operations such as data segment
// partitioning, truncating timestamps, time chunk segmentation or roll-up.
type GranularitySpec struct {
	Type               string `json:"type"`
	SegmentGranularity string `json:"segmentGranularity"`
	QueryGranularity   string `json:"queryGranularity"`
	Rollup             bool   `json:"rollup"`
}

type AutoScalerConfig struct {
	EnableTaskAutoScaler                 bool    `json:"enableTaskAutoScaler"`
	LagCollectionIntervalMillis          int     `json:"lagCollectionIntervalMillis"`
	LagCollectionRangeMillis             int     `json:"lagCollectionRangeMillis"`
	ScaleOutThreshold                    int     `json:"scaleOutThreshold"`
	TriggerScaleOutFractionThreshold     float64 `json:"triggerScaleOutFractionThreshold"`
	ScaleInThreshold                     int     `json:"scaleInThreshold"`
	TriggerScaleInFractionThreshold      float64 `json:"triggerScaleInFractionThreshold"`
	ScaleActionStartDelayMillis          int     `json:"scaleActionStartDelayMillis"`
	ScaleActionPeriodMillis              int     `json:"scaleActionPeriodMillis"`
	TaskCountMax                         int     `json:"taskCountMax"`
	TaskCountMin                         int     `json:"taskCountMin"`
	ScaleInStep                          int     `json:"scaleInStep"`
	ScaleOutStep                         int     `json:"scaleOutStep"`
	MinTriggerScaleActionFrequencyMillis int     `json:"minTriggerScaleActionFrequencyMillis"`
}

// Defines if and when stream Supervisor can become idle.
type IdleConfig struct {
	Enabled             bool  `json:"enabled"`
	InactiveAfterMillis int64 `json:"inactiveAfterMillis"`
}

// Firehose is an IOConfig firehose configuration
type Firehose struct {
	Type string `json:"type,omitempty"`

	// EventReceiverFirehoseFactory fields
	ServiceName string `json:"serviceName,omitempty"`
	BufferSize  int    `json:"bufferSize,omitempty"`
	MaxIdleTime int64  `json:"maxIdleTime,omitempty"`

	// FixedCountFirehoseFactory / ClippedFirehoseFactory / TimedShutoffFirehoseFactory fields
	Delegate    []Firehose `json:"delegate,omitempty"`
	Count       int        `json:"count,omitempty"`
	Interval    string     `json:"interval,omitempty"`
	ShutoffTime string     `json:"shutoffTime,omitempty"`
}

// CompactionInputSpec
type CompactionInputSpec struct {
	Type string `json:"type"`
	// CompactionIntervalSpec fields
	Interval                 string `json:"interval,omitempty"`
	Sha256OfSortedSegmentIds string `json:"sha256OfSortedSegmentIds,omitempty"`
	// SpecificSegmentsSpec fields
	Segments []string `json:"segments,omitempty"`
}

type MetadataStorageUpdaterJobSpec struct {
	Type           string         `json:"type"`
	ConnectURI     string         `json:"connectURI"`
	User           string         `json:"user"`
	Password       string         `json:"password"`
	SegmentTable   string         `json:"segmentTable"`
	CreteTable     bool           `json:"creteTable"`
	Host           string         `json:"host"`
	Port           string         `json:"port"`
	dbcpProperties map[string]any `json:"dbcp"`
}

// IOConfig influences how data is read into Druid from a source system.
type IOConfig struct {
	Type string `json:"type"`

	// IndexIOConfig / RealtimeIOConfig shared field
	Firehose *Firehose `json:"firehose,omitempty"`
	// IndexIOConfig field
	InputSource      *InputSource `json:"inputSource,omitempty"`
	AppendToExisting bool         `json:"appendToExisting,omitempty"`
	// IndexIOConfig / CompactionIOConfig shared field
	DropExisting bool `json:"dropExisting,omitempty"`

	// CompactionIOConfig / HadoopIOConfig fields
	InputSpec map[string]any `json:"inputSpec,omitempty"`

	// CompactionIOConfig field
	AllowNonAlignedInterval bool `json:"allowNonAlignedInterval,omitempty"`

	// HadoopIOConfig fields
	MetadataUpdateSpec *MetadataStorageUpdaterJobSpec `json:"metadataUpdateSpec,omitempty"`
	SegmentOutputPath  string                         `json:"segmentOutputPath,omitempty"`

	// KafkaIndexTaskIOConfig / KinesisIndexTaskIOConfig fields
	Topic              string              `json:"topic,omitempty"`
	ConsumerProperties *ConsumerProperties `json:"consumerProperties,omitempty"`
	TaskDuration       string              `json:"taskDuration,omitempty"`
	Replicas           int                 `json:"replicas,omitempty"`
	TaskCount          int                 `json:"taskCount,omitempty"`
	UseEarliestOffset  bool                `json:"useEarliestOffset"`

	AutoScalerConfig *AutoScalerConfig `json:"autoScalerConfig,omitempty"`

	TaskGroupID               int    `json:"taskGroupID,omitempty"`
	BaseSequenceName          string `json:"baseSequenceName,omitempty"`
	CompletionTimeout         string `json:"completionTimeout,omitempty"`
	PollTimeout               int    `json:"pollTimeout,omitempty"`
	StartDelay                string `json:"startDelay,omitempty"`
	Period                    string `json:"period,omitempty"`
	Stream                    string `json:"stream,omitempty"`
	UseEarliestSequenceNumber bool   `json:"useEarliestSequenceNumber,omitempty"`

	// common fields
	FlattenSpec *FlattenSpec `json:"flattenSpec,omitempty"`
	InputFormat *InputFormat `json:"inputFormat,omitempty"`
	IdleConfig  *IdleConfig  `json:"idleConfig,omitempty"`
}

// ConsumerProperties is a set of properties that is passed to a specific
// consumer, i.e. Kafka consumer.
type ConsumerProperties struct {
	BootstrapServers string `json:"bootstrap.servers,omitempty"`
}

// InputFormat specifies kafka messages format type.
// This can take values 'json', 'protobuf' or 'kafka'.
type InputFormat struct {
	Type string `json:"type"`

	// CsvInputFormat fields
	KeepNullColumns        bool `json:"keepNullColumns,omitempty"`
	AssumeNewlineDelimited bool `json:"assumeNewlineDelimited,omitempty"`
	UseJsonNodeReader      bool `json:"useJsonNodeReader,omitempty"`
}

type HttpInputSourceConfig struct {
	AllowedProtocols []string `json:" allowedProtocols,omitempty"`
}

type SplittableInputSource struct {
}

type InputSource struct {
	Type string `json:"type"`

	// LocalInputSource fields
	BaseDir string   `json:"baseDir,omitempty"`
	Filter  string   `json:"filter,omitempty"`
	Files   []string `json:"files,omitempty"`

	// HttpInputSource fields
	URIs                       []string               `json:"uris,omitempty"`
	HttpAuthenticationUsername string                 `json:"httpAuthenticationUsername,omitempty"`
	HttpAuthenticationPassword string                 `json:"httpAuthenticationPassword,omitempty"`
	HttpSourceConfig           *HttpInputSourceConfig `json:"config,omitempty"`

	// InlineInputSource fields
	Data string `json:"data,omitempty"`

	// CombiningInputSource fields
	Delegates []InputSource `json:"delegates,omitempty"`
}

// TransformSpec is responsible for transforming druid input data
// after it was read from kafka and after flattenSpec was applied.
type TransformSpec struct {
	Transforms TransformSet `json:"transforms"`
}

type SupervisorStatusPayload struct {
	Datasource      string `json:"dataSource"`
	Stream          string `json:"stream"`
	State           string `json:"state"`
	Partitions      int    `json:"partitions"`
	Replicas        int    `json:"replicas"`
	DurationSeconds int    `json:"durationSeconds"`
}

// SupervisorStatus is a response object of Druid SupervisorService's GetStatus method.
type SupervisorStatus struct {
	SupervisorId   string                   `json:"id"`
	GenerationTime time.Time                `json:"generationTime"`
	Payload        *SupervisorStatusPayload `json:"payload"`
}

type SupervisorState struct {
	ID            string `json:"id"`
	State         string `json:"state"`
	DetailedState string `json:"detailedState"`
	Healthy       bool   `json:"healthy"`
	Suspended     bool   `json:"suspended"`
}

type SupervisorStateWithSpec struct {
	ID            string              `json:"id"`
	State         string              `json:"state"`
	DetailedState string              `json:"detailedState"`
	Healthy       bool                `json:"healthy"`
	Suspended     bool                `json:"suspended"`
	Spec          *InputIngestionSpec `json:"spec"`
}

// defaultKafkaIngestionSpec returns a default InputIngestionSpec.
func defaultKafkaIngestionSpec() *InputIngestionSpec {
	spec := &InputIngestionSpec{
		Type: "kafka",
		DataSchema: &DataSchema{
			DataSource: "test",
			TimeStampSpec: &TimestampSpec{
				Column: "ts",
				Format: "auto",
			},
			TransformSpec: &TransformSpec{
				Transforms: []Transform{
					{
						Type: "expression",
						Name: "payload",
						Expr: "parse_json(payload)",
					},
				},
			},
			DimensionsSpec: &DimensionsSpec{
				Dimensions: DimensionSet{
					"id",
					"ts",
					"payload",
				},
			},
			GranularitySpec: &GranularitySpec{
				Type:               "uniform",
				SegmentGranularity: "DAY",
				QueryGranularity:   "none",
				Rollup:             false,
			},
		},
		IOConfig: &IOConfig{
			Type:  "kafka",
			Topic: "",
			InputFormat: &InputFormat{
				Type: "json",
			},
			TaskDuration: "PT30M",
			ConsumerProperties: &ConsumerProperties{
				BootstrapServers: "",
			},
			UseEarliestOffset: false,
			FlattenSpec: &FlattenSpec{
				Fields: FieldList{},
			},
		},
	}
	return spec
}

// NewIngestionSpec returns a default InputIngestionSpec and applies any
// options passed to it.
func NewIngestionSpec(options ...IngestionSpecOptions) *InputIngestionSpec {
	spec := defaultKafkaIngestionSpec()
	for _, fn := range options {
		fn(spec)
	}
	return spec
}

// IngestionSpecOptions allows for configuring a InputIngestionSpec.
type IngestionSpecOptions func(*InputIngestionSpec)

// SetType sets the type of the supervisor (IOConfig).
func SetType(stype string) IngestionSpecOptions {
	return func(spec *InputIngestionSpec) {
		if stype != "" {
			spec.IOConfig.Type = stype
		}
	}
}

// SetTopic sets the Kafka topic to consume data from.
func SetTopic(topic string) IngestionSpecOptions {
	return func(spec *InputIngestionSpec) {
		if topic != "" {
			spec.IOConfig.Topic = topic
		}
	}
}

// SetDataSource sets the name of the dataSource used in Druid.
func SetDataSource(ds string) IngestionSpecOptions {
	return func(spec *InputIngestionSpec) {
		if ds != "" {
			spec.DataSchema.DataSource = ds
		}
	}
}

// SetInputFormat sets the input format type, i.e. json, protobuf etc.
func SetInputFormat(format string) IngestionSpecOptions {
	return func(spec *InputIngestionSpec) {
		if format != "" {
			spec.IOConfig.InputFormat.Type = format
		}
	}
}

// SetBrokers sets the addresses of Kafka brokers. in the list form: 'kafka01:9092,
// kafka02:9092,kafka03:9092' or as a cluster DNS: ”.
func SetBrokers(brokers string) IngestionSpecOptions {
	return func(spec *InputIngestionSpec) {
		if brokers != "" {
			spec.IOConfig.ConsumerProperties.BootstrapServers = brokers
		}
	}
}

// SetTaskDuration sets the upper limit for druid ingestion task.
func SetTaskDuration(duration string) IngestionSpecOptions {
	return func(spec *InputIngestionSpec) {
		if duration != "" {
			spec.IOConfig.TaskDuration = duration
		}
	}
}

// SetDimensions sets druid datasource dimensions.
func SetDimensions(dimensions DimensionSet) IngestionSpecOptions {
	return func(spec *InputIngestionSpec) {
		spec.DataSchema.DimensionsSpec.Dimensions = dimensions
	}
}

// SetUseEarliestOffset configures kafka druid ingestion supervisor to start reading
// from the earliest or latest offsets in Kafka.
func SetUseEarliestOffset(useEarliestOffset bool) IngestionSpecOptions {
	return func(spec *InputIngestionSpec) {
		spec.IOConfig.UseEarliestOffset = useEarliestOffset
	}
}
