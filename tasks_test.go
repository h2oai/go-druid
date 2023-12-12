package druid

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx/types"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
)

// testDO represents entry with payload.
type testDO struct {
	Timestamp time.Time      `db:"ts"`
	Id        uuid.UUID      `db:"id"`
	Payload   types.JSONText `db:"payload"`
}

var testObjects = []testDO{
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

// TriggerIngestionTask initiates inline ingestion task with druid client.
func TriggerIngestionTask[T any](d *Client, dataSourceName string, entries []T) (string, error) {
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
		SetTaskInputFormat("csv", "false", []string{"ts", "id", "payload"}),
		SetTaskInlineInputData(csvEntriesBuff.String()),
	)
	taskID, err := d.Tasks().SubmitTask(spec)
	return taskID, err
}

// AwaitTaskCompletion waits for the task to complete. Function timeouts with an error after awaitTimeout nanoseconds.
func AwaitTaskCompletion(client *Client, taskID string, awaitTimeout time.Duration, tickerDuration time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), awaitTimeout)
	defer cancel()

	ticker := time.NewTicker(tickerDuration)
	defer ticker.Stop()
L:
	for {
		select {
		case <-ticker.C:
			res, err := client.Tasks().GetStatus(taskID)
			if err != nil {
				return err
			}

			if res.Status.Status == "RUNNING" {
				continue
			}
			break L
		case <-ctx.Done():
			return errors.New("AwaitTaskRunning timeout")
		}
	}
	return nil
}

// AwaitTaskStatus waits for the druid task status for the maximum of awaitTimeout duration, querying druid task API.
func AwaitTaskStatus(client *Client, taskID string, status string, awaitTimeout time.Duration, tickerDuration time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), awaitTimeout)
	defer cancel()

	ticker := time.NewTicker(tickerDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			res, err := client.Tasks().GetStatus(taskID)
			if err != nil {
				return err
			}

			if res.Status.Status == status {
				return nil
			}
		case <-ctx.Done():
			return errors.New("AwaitTaskRunning timeout")
		}
	}
}

// RunInlineIngestionTask initiates inline ingestion task with druid client and runs until it is complete.
func RunInlineIngestionTask[T any](client *Client, dataSourceName string, entries []T, recordsCount int) error {
	taskID, err := TriggerIngestionTask(client, dataSourceName, entries)
	if err != nil {
		return err
	}

	err = AwaitTaskCompletion(client, taskID, 180*time.Second, 100*time.Millisecond)
	if err != nil {
		return err
	}

	err = client.Metadata(WithMetadataQueryTicker(200*time.Millisecond), WithMetadataQueryTimeout(120*time.Second)).AwaitDataSourceAvailable(dataSourceName)
	if err != nil {
		return err
	}

	err = client.Metadata().AwaitRecordsCount(dataSourceName, recordsCount)
	if err != nil {
		return err
	}

	return nil
}

func TestTaskService(t *testing.T) {
	// Set up druid containers using docker-compose.
	compose, err := tc.NewDockerCompose("testdata/docker-compose.yaml")
	require.NoError(t, err, "NewDockerComposeAPI()")

	// Set up cleanup for druid containers.
	t.Cleanup(func() {
		require.NoError(t, compose.Down(context.Background(), tc.RemoveOrphans(true), tc.RemoveVolumes(true), tc.RemoveImagesLocal), "compose.Down()")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up druid service and client.
	d, err := NewClient("http://localhost:8888")
	require.NoError(t, err, "error should be nil")

	// Waiting for druid services to start.
	err = compose.
		WaitForService("coordinator", wait.NewHTTPStrategy(processInformationPathPrefix).WithPort("8081/tcp").WithStartupTimeout(180*time.Second)).
		WaitForService("router", wait.NewHTTPStrategy(processInformationPathPrefix).WithPort("8888/tcp").WithStartupTimeout(180*time.Second)).
		WaitForService("broker", wait.NewHTTPStrategy(processInformationPathPrefix).WithPort("8082/tcp").WithStartupTimeout(180*time.Second)).
		WaitForService("middlemanager", wait.NewHTTPStrategy(processInformationPathPrefix).WithPort("8091/tcp").WithStartupTimeout(180*time.Second)).
		Up(ctx, tc.Wait(true))
	require.NoError(t, err, "druid services should be up with no error")

	// Test create ingestion task -> get status -> complete sequence.
	RunInlineIngestionTask(d, "test-submit-task-datasource", testObjects, 2)
	require.NoError(t, err, "error should be nil")
}

func TestTerminateTask(t *testing.T) {
	// Set up druid containers using docker-compose.
	compose, err := tc.NewDockerCompose("testdata/docker-compose.yaml")
	require.NoError(t, err, "NewDockerComposeAPI()")

	// Set up cleanup for druid containers.
	t.Cleanup(func() {
		require.NoError(t, compose.Down(context.Background(), tc.RemoveOrphans(true), tc.RemoveVolumes(true), tc.RemoveImagesLocal), "compose.Down()")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up druid service and client.
	d, err := NewClient("http://localhost:8888")
	require.NoError(t, err, "error should be nil")

	// Waiting for druid services to start.
	err = compose.
		WaitForService("coordinator", wait.NewHTTPStrategy(processInformationPathPrefix).WithPort("8081/tcp").WithStartupTimeout(180*time.Second)).
		WaitForService("router", wait.NewHTTPStrategy(processInformationPathPrefix).WithPort("8888/tcp").WithStartupTimeout(180*time.Second)).
		WaitForService("broker", wait.NewHTTPStrategy(processInformationPathPrefix).WithPort("8082/tcp").WithStartupTimeout(180*time.Second)).
		WaitForService("middlemanager", wait.NewHTTPStrategy(processInformationPathPrefix).WithPort("8091/tcp").WithStartupTimeout(180*time.Second)).
		Up(ctx, tc.Wait(true))
	require.NoError(t, err, "druid services should be up with no error")

	// Test create ingestion task -> get status -> terminate sequence.
	taskID, err := TriggerIngestionTask(d, "test-terminate-task-datasource", testObjects)
	require.NoError(t, err, "error should be nil")

	err = AwaitTaskStatus(d, taskID, "RUNNING", 180*time.Second, 200*time.Millisecond)
	require.NoError(t, err, "error should be nil")

	shutdownTaskID, err := d.Tasks().Shutdown(taskID)
	require.NoError(t, err, "error should be nil")
	require.Equal(t, shutdownTaskID, taskID)
}
