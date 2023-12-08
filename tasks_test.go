package druid

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx/types"
	"github.com/stretchr/testify/assert"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestDO represents entry with payload.
type TestDO struct {
	Timestamp time.Time      `db:"ts"`
	Id        uuid.UUID      `db:"id"` // output only
	Payload   types.JSONText `db:"payload"`
}

var testObjects = []TestDO{
	{
		Id:        uuid.New(),
		Timestamp: time.Now(),
		Payload:   types.JSONText("{\"test\": \"json\"}"),
	},
	{
		Id:        uuid.New(),
		Timestamp: time.Now().Add(time.Hour),
		Payload:   types.JSONText("{\"test\": \"json2\"}"),
	},
}

func triggerIngestionTask(d *Client, dataSourceName string, entries []TestDO) (string, error) {
	csvEntriesBuff := &bytes.Buffer{}

	err := gocsv.MarshalWithoutHeaders(entries, csvEntriesBuff)
	if err != nil {
		return "", err
	}

	var spec = NewTaskIngestionSpec(
		SetTaskType("index_parallel"),
		SetTaskDataSource(dataSourceName),
		SetTaskTuningConfig("index_parallel", 25000, 5000000),
		SetTaskIOConfigType("index_parallel"),
		SetTaskInputFormat("csv", "false", []string{}),
		SetTaskInlineInputData(csvEntriesBuff.String()),
	)
	taskID, err := d.Tasks().SubmitTask(spec)
	return taskID, err
}

func TestTaskService(t *testing.T) {
	// Set up druid containers using docker-compose.
	compose, err := tc.NewDockerCompose("testdata/docker-compose.yaml")
	assert.NoError(t, err, "NewDockerComposeAPI()")

	// Set up cleanup for druid containers.
	t.Cleanup(func() {
		assert.NoError(t, compose.Down(context.Background(), tc.RemoveOrphans(true), tc.RemoveVolumes(true), tc.RemoveImagesLocal), "compose.Down()")
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Wait for druid contaners to start.
	assert.NoError(t, compose.Up(ctx, tc.Wait(true)), "compose.Up()")

	// Set up druid service and client.
	var druidOpts []ClientOption
	d, err := NewClient("http://localhost:8091", druidOpts...)
	assert.NoError(t, err, "error should be nil")

	// Waiting for druid coordinator service to start.
	err = compose.
		WaitForService("coordinator", wait.NewHTTPStrategy(processInformationPathPrefix).WithPort("8081/tcp").WithStartupTimeout(180*time.Second)).
		WaitForService("router", wait.NewHTTPStrategy(processInformationPathPrefix).WithPort("8888/tcp").WithStartupTimeout(180*time.Second)).
		WaitForService("broker", wait.NewHTTPStrategy(processInformationPathPrefix).WithPort("8082/tcp").WithStartupTimeout(180*time.Second)).
		Up(ctx, tc.Wait(true))
	assert.NoError(t, err, "coordinator should be up with no error")

	// Test create supervisor -> get status -> terminate sequence.
	triggerIngestionTask(d, "test-datasource", testObjects)
	assert.NoError(t, err, "error should be nil")
}
