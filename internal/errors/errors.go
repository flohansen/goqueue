package errors

import "fmt"

type ErrNoJob struct {
}

func (e *ErrNoJob) Error() string {
	return "no job available"
}

type ErrReceiveJob struct {
	Err error
}

func (e *ErrReceiveJob) Error() string {
	return fmt.Sprintf("failed to get job: %v", e.Err)
}

type ErrUpdateJobState struct {
	CurrentState string
	WantedState  string
	Err          error
}

func (e *ErrUpdateJobState) Error() string {
	return fmt.Sprintf("failed to update job state from %q to %q: %v", e.CurrentState, e.WantedState, e.Err)
}

type ErrWorkerFailed struct {
	Err error
}

func (e *ErrWorkerFailed) Error() string {
	return fmt.Sprintf("failed to execute job: %v", e.Err)
}
