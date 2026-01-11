package goqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/flohansen/goqueue/internal/database"
	internalerrs "github.com/flohansen/goqueue/internal/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// Worker processes jobs ob type T. Implementations should be idempotent as jobs
// may be retried multiple times on failure.
type Worker[T any] interface {
	// Work processes a single job. Returning an error triggers retry logic
	// based on the job's retry policy and max retry count. If max retries is
	// exceeded, the job is marked as failed.
	Work(ctx context.Context, job *Job[T]) error
}

// JobQueue manages the lifecycle of jobs in a named queue. It polls the
// database for scheduled jobs, executes them via a Worker, and handles retries
// with exponential backoff or other configured policies. Jobs are processed
// concurrently.
//
// T is the type of the job arguments payload and must be JSON serializable.
//
// JobQueues should be created using the New function. Example:
//
//	queue := jobqueue.New(db, &MyWorker{},
//	    jobqueue.WithQueueName("my-queue"),
//	    jobqueue.WithPollInterval(500*time.Millisecond),
//	)
type JobQueue[T any] struct {
	db             database.DBTX
	q              database.Querier
	worker         Worker[T]
	queueName      string
	logger         *slog.Logger
	pollInterval   time.Duration
	baseRetryDelay time.Duration
	maxRetryDelay  time.Duration
}

// New creates a new JobQueue with the given database connection and worker.
// Configure behavior using functional options like WithPollInterval,
// WithQueueName, etc.
func New[T any](db database.DBTX, worker Worker[T], opts ...JobQueueOption) *JobQueue[T] {
	cfg := &jobQueueConfig{
		queueName:      "default",
		logger:         slog.Default(),
		pollInterval:   1 * time.Second,
		baseRetryDelay: 2 * time.Second,
		maxRetryDelay:  1 * time.Hour,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	return &JobQueue[T]{
		db:             db,
		q:              database.New(db),
		worker:         worker,
		queueName:      cfg.queueName,
		logger:         cfg.logger,
		pollInterval:   cfg.pollInterval,
		baseRetryDelay: cfg.baseRetryDelay,
		maxRetryDelay:  cfg.maxRetryDelay,
	}
}

// Job represents a queued job with its arguments and database ID.
type Job[T any] struct {
	// ID is the database identifier for the job.
	ID int32

	// Args contains the job payload of type T.
	Args T
}

// Enqueue adds a new job with the given arguments to the queue. Optional
// parameters can be set using EnqueueOption functions like WithMaxRetries
// and WithRetryPolicy. It returns the created Job or an error. Example:
//
//	job, err := queue.Enqueue(ctx, MyJobArgs{...},
//	    jobqueue.WithMaxRetries(5),
//	    jobqueue.WithRetryPolicy(jobqueue.RetryPolicyExponential),
//	)
func (jq *JobQueue[T]) Enqueue(ctx context.Context, args T, opts ...EnqueueOption) (*Job[T], error) {
	b, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("failed to json encode job arguments: %w", err)
	}

	cfg := &enqueueConfig{
		maxRetries:  3,
		retryPolicy: database.GoqueueRetryPolicyExponential,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	job, err := jq.q.InsertJob(ctx, database.InsertJobParams{
		QueueName:   jq.queueName,
		Arguments:   b,
		MaxRetries:  cfg.maxRetries,
		RetryPolicy: cfg.retryPolicy,
		ScheduledAt: pgtype.Timestamp{Time: time.Now().UTC(), Valid: true},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to insert job: %w", err)
	}

	return &Job[T]{
		ID:   job.JobID,
		Args: args,
	}, nil
}

// Receive starts polling the queue for scheduled jobs and processes them using
// the configured Worker. It runs until the provided context is cancelled.
// Errors during job processing are logged but do not stop the polling loop. Failed
// jobs are retried or marked as failed after the retry policy has been exhausted.
//
// Example:
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	go queue.Receive(ctx)
//
//	// Run for 10 minutes
//	time.Sleep(10 * time.Minute)
//	cancel()
func (jq *JobQueue[T]) Receive(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(jq.pollInterval):
			if err := jq.receive(ctx); err != nil {
				var t *internalerrs.NoJobError
				if errors.As(err, &t) {
					continue
				}

				jq.logger.Error("receive error", "error", err)
			}
		}
	}
}

// receive fetches and processes a single job from the queue. It returns an error
// if job processing fails or if no job is available. This method is called by
// the Receive loop. Errors are categorized for proper handling. Retry logic and
// failure marking are handled here.
func (jq *JobQueue[T]) receive(ctx context.Context) error {
	job, err := jq.q.FetchJobLocked(ctx, jq.queueName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &internalerrs.NoJobError{}
		}

		return &internalerrs.ReceiveJobError{Err: err}
	}

	var args T
	if err := json.Unmarshal(job.Arguments, &args); err != nil {
		jq.markJobFailed(ctx, job, err)
		return &internalerrs.ReceiveJobError{Err: err}
	}

	if err := jq.worker.Work(ctx, &Job[T]{
		ID:   job.JobID,
		Args: args,
	}); err != nil {
		if job.RetryAttempt < job.MaxRetries {
			jq.retryJob(ctx, job)
		} else {
			jq.markJobFailed(ctx, job, errors.New("maximum number of retries reached"))
		}
		return &internalerrs.WorkerFailedError{Err: err}
	}

	if _, err := jq.q.UpdateJobFinished(ctx, job.JobID); err != nil {
		return &internalerrs.UpdateJobStatusError{
			CurrentState: string(job.Status),
			WantedState:  string(database.GoqueueJobStatusFinished),
			Err:          err,
		}
	}

	return nil
}

// retryJob reschedules the given job for a future time based on its retry
// policy and the number of attempts already made. If rescheduling fails, an
// error is logged.
func (jq *JobQueue[T]) retryJob(ctx context.Context, job database.GoqueueJob) {
	nextRetryDelay := jq.calcNextRetryDelay(job)
	if _, err := jq.q.RescheduleJob(ctx, database.RescheduleJobParams{
		JobID:       job.JobID,
		ScheduledAt: pgtype.Timestamp{Time: time.Now().Add(nextRetryDelay).UTC(), Valid: true},
	}); err != nil {
		jq.logger.Error("failed to retry job", "job_id", job.JobID, "error", err)
	}
}

// markJobFailed updates the job status to 'failed' with the provided error
// message. If the update fails, an error is logged.
func (jq *JobQueue[T]) markJobFailed(ctx context.Context, job database.GoqueueJob, err error) {
	if _, err := jq.q.UpdateJobFailed(ctx, database.UpdateJobFailedParams{
		JobID: job.JobID,
		Error: pgtype.Text{String: err.Error(), Valid: true},
	}); err != nil {
		jq.logger.Error("failed to update job status to 'failed'", "job_id", job.JobID, "error", err)
	}
}

// calcNextRetryDelay calculates the delay before the next retry attempt
// based on the job's retry policy and the number of attempts already made.
func (jq *JobQueue[T]) calcNextRetryDelay(job database.GoqueueJob) time.Duration {
	delay := jq.baseRetryDelay

	switch job.RetryPolicy {
	case database.GoqueueRetryPolicyConstant:
	case database.GoqueueRetryPolicyLinear:
		delay = delay * time.Duration(job.RetryAttempt+1)
	case database.GoqueueRetryPolicyExponential:
		delay = time.Duration(math.Pow(delay.Seconds(), float64(job.RetryAttempt+1))) * time.Second
	}

	if delay > jq.maxRetryDelay {
		delay = jq.maxRetryDelay
	}

	return delay
}

// jobQueueConfig holds configuration options for JobQueue.
type jobQueueConfig struct {
	queueName      string
	logger         *slog.Logger
	pollInterval   time.Duration
	baseRetryDelay time.Duration
	maxRetryDelay  time.Duration
}

// JobQueueOption defines a functional option for configuring a JobQueue.
type JobQueueOption func(*jobQueueConfig)

// WithLogger sets a custom logger for the JobQueue.
func WithLogger(logger *slog.Logger) JobQueueOption {
	return func(jqc *jobQueueConfig) {
		jqc.logger = logger
	}
}

// WithPollInterval sets the polling interval for checking the queue for new jobs.
func WithPollInterval(interval time.Duration) JobQueueOption {
	return func(jqc *jobQueueConfig) {
		jqc.pollInterval = interval
	}
}

// WithBaseRetryDelay sets the base delay used for calculating retry backoff.
func WithBaseRetryDelay(d time.Duration) JobQueueOption {
	return func(jqc *jobQueueConfig) {
		jqc.baseRetryDelay = d
	}
}

// WithMaxRetryDelay sets the maximum delay allowed between retry attempts.
func WithMaxRetryDelay(d time.Duration) JobQueueOption {
	return func(jqc *jobQueueConfig) {
		jqc.maxRetryDelay = d
	}
}

// WithQueueName sets the name of the job queue.
func WithQueueName(name string) JobQueueOption {
	return func(jqc *jobQueueConfig) {
		jqc.queueName = name
	}
}

// enqueueConfig holds configuration options for enqueuing a job.
type enqueueConfig struct {
	maxRetries  int32
	retryPolicy database.GoqueueRetryPolicy
}

// EnqueueOption defines a functional option for configuring job enqueuing.
type EnqueueOption func(*enqueueConfig)

// WithMaxRetries sets the maximum number of retries for the enqueued job.
func WithMaxRetries(n int32) EnqueueOption {
	return func(ec *enqueueConfig) {
		ec.maxRetries = n
	}
}

// WithRetryPolicy sets the retry policy for the enqueued job. Possible policies
// are RetryPolicyConstant, RetryPolicyLinear, and RetryPolicyExponential.
func WithRetryPolicy(policy RetryPolicy) EnqueueOption {
	return func(ec *enqueueConfig) {
		switch policy {
		case RetryPolicyConstant:
			ec.retryPolicy = database.GoqueueRetryPolicyConstant
		case RetryPolicyLinear:
			ec.retryPolicy = database.GoqueueRetryPolicyLinear
		case RetryPolicyExponential:
			ec.retryPolicy = database.GoqueueRetryPolicyExponential
		}
	}
}

// RetryPolicy defines the strategy for retrying failed jobs.
type RetryPolicy int

const (
	// RetryPolicyConstant retries jobs with a constant delay. Formlar:
	//
	//	next_delay = base_delay
	//
	RetryPolicyConstant RetryPolicy = iota

	// RetryPolicyLinear retries jobs with a linearly increasing delay. Formula:
	//
	//	next_delay = base_delay * (attempt_number + 1)
	//
	RetryPolicyLinear

	// RetryPolicyExponential retries jobs with an exponentially increasing delay. Formula:
	//
	//	next_delay = base_delay ^ (attempt_number + 1)
	//
	RetryPolicyExponential
)
