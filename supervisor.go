package druid

import (
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	supervisorPathPrefix             = "druid/indexer/v1/supervisor"
	supervisorSpecPathPrefix         = "druid/indexer/v1/supervisor/:supervisorId"
	supervisorAllActivePathPrefix    = "druid/indexer/v1/supervisor?full"
	supervisorStatusPathPrefix       = "druid/indexer/v1/supervisor/:supervisorId/status"
	supervisorHistoryAllPathPrefix   = "druid/indexer/v1/supervisor/history"
	supervisorHistoryPathPrefix      = "druid/indexer/v1/supervisor/:supervisorId/history"
	supervisorSuspendPathPrefix      = "druid/indexer/v1/supervisor/:supervisorId/suspend"
	supervisorSuspendAllPathPrefix   = "druid/indexer/v1/supervisor/suspendAll"
	supervisorResumePathPrefix       = "druid/indexer/v1/supervisor/:supervisorId/resume"
	supervisorResumeAllPathPrefix    = "druid/indexer/v1/supervisor/resumeAll"
	supervisorResetPathPrefix        = "druid/indexer/v1/supervisor/:supervisorId/reset"
	supervisorTerminatePathPrefix    = "druid/indexer/v1/supervisor/:supervisorId/terminate"
	supervisorTerminateAllPathPrefix = "druid/indexer/v1/supervisor/terminateAll"
	supervisorShutdownPathPrefix     = "druid/indexer/v1/supervisor/:supervisorId/shutdown"
)

// SupervisorService is a service that submits ingestion tasks to druid supervisor API.
type SupervisorService struct {
	client *Client
}

// CreateSupervisorResponse is a response object of Druid SupervisorService's SubmitTask method
type CreateSupervisorResponse struct {
	SupervisorId string `json:"id"`
}

// TerminateSupervisorResponse is a response object of Druid SupervisorService's Terminate method
type TerminateSupervisorResponse struct {
	SupervisorId string `json:"id"`
}

type GetStatusResponsePayload struct {
	Datasource      string `json:"dataSource"`
	Stream          string `json:"stream"`
	State           string `json:"state"`
	Partitions      int    `json:"partitions"`
	Replicas        int    `json:"replicas"`
	DurationSeconds int    `json:"durationSeconds"`
}

// GetStatusResponse is a response object of Druid SupervisorService's GetStatus method
type GetStatusResponse struct {
	SupervisorId   string                   `json:"id"`
	GenerationTime time.Time                `json:"generationTime"`
	Payload        GetStatusResponsePayload `json:"payload"`
}

// CreateOrUpdate submits an ingestion specification to druid Supervisor API with a pre-configured druid client
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#create-or-update-a-supervisor
func (s *SupervisorService) CreateOrUpdate(spec interface{}) (string, error) {
	r, err := s.client.NewRequest("POST", supervisorPathPrefix, spec)
	if err != nil {
		return "", err
	}
	var result CreateSupervisorResponse
	_, err = s.client.Do(r, &result)
	if err != nil {
		return "", err
	}
	return result.SupervisorId, nil
}

func applySupervisorId(input string, supervisorId string) string {
	return strings.Replace(input, ":supervisorId", supervisorId, 1)
}

// GetActiveIDs returns array of active supervisor IDs
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#get-an-array-of-active-supervisor-ids
func (s *SupervisorService) GetActiveIDs() ([]string, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetActiveIDs not implemented")
}

// GetAllActiveSpecs returns array of active supervisor objects
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#get-an-array-of-active-supervisor-objects
func (s *SupervisorService) GetAllActiveSpecs() ([]interface{}, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAllActiveSpecs not implemented")
}

// GetAllActiveSpecs returns array of active supervisor objects
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#get-an-array-of-active-supervisor-objects
func (s *SupervisorService) GetAllActiveStates() ([]string, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAllActiveStates not implemented")
}

// GetSpec calls druid Supervisor Status API
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#get-supervisor-specification
func (s *SupervisorService) GetSpec(supervisorId string) (interface{}, error) {
	r, err := s.client.NewRequest("GET", applySupervisorId(supervisorSpecPathPrefix, supervisorId), nil)
	var result interface{}
	if err != nil {
		return result, err
	}
	_, err = s.client.Do(r, &result)
	if err != nil {
		return result, err
	}
	return result, nil
}

// GetStatus calls druid Supervisor service's Get status API
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#get-supervisor-status
func (s *SupervisorService) GetStatus(supervisorId string) (GetStatusResponse, error) {
	r, err := s.client.NewRequest("GET", applySupervisorId(supervisorStatusPathPrefix, supervisorId), nil)
	var result GetStatusResponse
	if err != nil {
		return result, err
	}
	_, err = s.client.Do(r, &result)
	if err != nil {
		return result, err
	}
	return result, nil
}

// GetAuditHistory calls druid Supervisor Status API
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#get-audit-history-for-a-specific-supervisor
func (s *SupervisorService) GetAuditHistory(_supervisorId string) (interface{}, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAuditHistory not implemented")
}

// GetAuditHistoryAll calls druid Supervisor Status API
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#get-supervisor-specification
func (s *SupervisorService) GetAuditHistoryAll() ([]interface{}, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAuditHistoryAll not implemented")
}

// Suspend calls druid Supervisor service's Suspend API
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#suspend-a-running-supervisor
func (s *SupervisorService) Suspend(_supervisorId string) (interface{}, error) {
	return "", status.Errorf(codes.Unimplemented, "method Suspend not implemented")
}

// SuspendAll calls druid Supervisor service's SuspendAll API
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#suspend-all-supervisors
func (s *SupervisorService) SuspendAll() (string, error) {
	return "", status.Errorf(codes.Unimplemented, "method SuspendAll not implemented")
}

// Resume calls druid Supervisor service's Resume API
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#resume-a-supervisor
func (s *SupervisorService) Resume(_supervisorId string) (interface{}, error) {
	return "", status.Errorf(codes.Unimplemented, "method Resume not implemented")
}

// ResumeAll calls druid Supervisor service's ResumeAll API
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#resume-all-supervisors
func (s *SupervisorService) ResumeAll() (string, error) {
	return "", status.Errorf(codes.Unimplemented, "method ResumeAll not implemented")
}

// Reset calls druid Supervisor service's Shutdown API
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#reset-a-supervisor
func (s *SupervisorService) Reset(_supervisorId string) (string, error) {
	return "", status.Errorf(codes.Unimplemented, "method Reset not implemented")
}

// Terminate calls druid Supervisor service's Terminate API
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#terminate-a-supervisor
func (s *SupervisorService) Terminate(supervisorId string) (string, error) {
	r, err := s.client.NewRequest("POST", applySupervisorId(supervisorTerminatePathPrefix, supervisorId), "")
	if err != nil {
		return "", err
	}
	var result TerminateSupervisorResponse
	_, err = s.client.Do(r, &result)
	if err != nil {
		return "", err
	}
	return result.SupervisorId, nil
}

// TerminateAll calls druid Supervisor service's TerminateAll API
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#terminate-all-supervisors
func (s *SupervisorService) TerminateAll() (string, error) {
	return "", status.Errorf(codes.Unimplemented, "method TerminateAll not implemented")
}

// Shutdown calls druid Supervisor service's Shutdown API
// https://druid.apache.org/docs/latest/api-reference/supervisor-api/#shut-down-a-supervisor
func (s *SupervisorService) Shutdown(_supervisorId string) (string, error) {
	return "", status.Errorf(codes.Unimplemented, "method Shutdown not implemented")
}
