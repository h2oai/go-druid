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

func triggerIngestionTask(d *Client, dataSourceName string, entries []testDO) (string, error) {
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

type testFunction func() error

func awaitTaskCompletion(client *Client, taskID string, durationSeconds int, tickDurationMilliseconds time.Duration) error {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(time.Duration(durationSeconds) * time.Second)
		cancel()
	}()
	ticker := time.NewTicker(tickDurationMilliseconds * time.Millisecond)
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
			break
		case <-ctx.Done():
			return errors.New("awaitTaskCompletion timeout")
		}
	}
}

func awaitTaskRunning(client *Client, taskID string, durationSeconds int, tickDurationMilliseconds time.Duration) error {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(time.Duration(durationSeconds) * time.Second)
		cancel()
	}()
	ticker := time.NewTicker(tickDurationMilliseconds * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			res, err := client.Tasks().GetStatus(taskID)
			if err != nil {
				return err
			}

			if res.Status.Status == "RUNNING" {
				return nil
			}
		case <-ctx.Done():
			return errors.New("awaitTaskCompletion timeout")
		}
	}
}

func runInlineIngestionTask(client *Client, dataSourceName string, entries []testDO, recordsCount int) error {
	taskID, err := triggerIngestionTask(client, dataSourceName, entries)
	if err != nil {
		return err
	}

	err = awaitTaskCompletion(client, taskID, 180, 500)
	if err != nil {
		return err
	}

	err = client.Metadata(180, 500).AwaitDataSourceAvailable(dataSourceName)
	if err != nil {
		return err
	}

	err = client.Metadata(180, 500).AwaitRecordsCount(dataSourceName, recordsCount)
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
	runInlineIngestionTask(d, "test-submit-task-datasource", testObjects, 2)
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
	taskID, err := triggerIngestionTask(d, "test-terminate-task-datasource", testObjects)
	require.NoError(t, err, "error should be nil")

	err = awaitTaskRunning(d, taskID, 180, 500)
	require.NoError(t, err, "error should be nil")

	shutdownTaskID, err := d.Tasks().Shutdown(taskID)
	require.NoError(t, err, "error should be nil")
	require.Equal(t, shutdownTaskID, taskID)
}
