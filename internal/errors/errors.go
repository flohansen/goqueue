package errors

import "fmt"

type NoJobError struct {
}

func (e *NoJobError) Error() string {
	return "no job available"
}

type ReceiveJobError struct {
	Err error
}

func (e *ReceiveJobError) Error() string {
	return fmt.Sprintf("failed to get job: %v", e.Err)
}

type UpdateJobStatusError struct {
	CurrentState string
	WantedState  string
	Err          error
}

func (e *UpdateJobStatusError) Error() string {
	return fmt.Sprintf("failed to update job state from %q to %q: %v", e.CurrentState, e.WantedState, e.Err)
}

type WorkerFailedError struct {
	Err error
}

func (e *WorkerFailedError) Error() string {
	return fmt.Sprintf("failed to execute job: %v", e.Err)
}
