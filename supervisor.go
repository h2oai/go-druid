package druid

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LongRunningOperation represents a druid long-running operation submitted to a druid Supervisor API.
type LongRunningOperation struct {
	client *Client
	taskId string
	status string
}

// DruidSupervisorService is a service that submits ingestion tasks to druid supervisor API.
type SupervisorService struct {
	client *Client
}

// DruidSupervisorSubmitTaskResponse is a response object of DruidSupervisorService's SubmitIngestionTask method
type DruidSupervisorSubmitTaskResponse struct {
	SupervisorId string `json:"id"`
}

// SubmitIngestionTask submits an ingestion specification to druid Supervisor API with a pre-configured druid client
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#create-or-update-a-supervisor
func (ds *SupervisorService) SubmitIngestionTask(spec interface{}) (*LongRunningOperation, error) {
	r, err := ds.client.NewRequest("POST", supervisorPathPrefix, spec)
	if err != nil {
		return nil, err
	}
	var result DruidSupervisorSubmitTaskResponse
	_, err = ds.client.Do(r, result)
	if err != nil {
		return nil, err
	}
	return &LongRunningOperation{ds.client, result.SupervisorId, "RUNNING"}, nil
}

// GetOperationStatus will call druid Supervisor Status API
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#get-supervisor-status
func (*LongRunningOperation) GetOperationStatus() (*LongRunningOperation, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetOperationStatus not implemented")
}

// CancelOperation will call druid Supervisor Terminate API
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#terminate-a-supervisor
func (*LongRunningOperation) CancelOperation() error {
	return status.Errorf(codes.Unimplemented, "method CancelOperation not implemented")
}
