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

type DB interface {
	database.DBTX
	BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error)
}

type Worker[T any] interface {
	Work(ctx context.Context, job *Job[T]) error
}

type JobQueue[T any] struct {
	db             DB
	q              database.Querier
	worker         Worker[T]
	queueName      string
	logger         *slog.Logger
	pollInterval   time.Duration
	baseRetryDelay time.Duration
	maxRetryDelay  time.Duration
}

func New[T any](db DB, worker Worker[T], opts ...JobQueueOption) *JobQueue[T] {
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

type Job[T any] struct {
	ID   int32
	Args T
}

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

func (jq *JobQueue[T]) retryJob(ctx context.Context, job database.GoqueueJob) {
	nextRetryDelay := jq.calcNextRetryDelay(job)
	if _, err := jq.q.RescheduleJob(ctx, database.RescheduleJobParams{
		JobID:       job.JobID,
		ScheduledAt: pgtype.Timestamp{Time: time.Now().Add(nextRetryDelay).UTC(), Valid: true},
	}); err != nil {
		jq.logger.Error("failed to retry job", "job_id", job.JobID, "error", err)
	}
}

func (jq *JobQueue[T]) markJobFailed(ctx context.Context, job database.GoqueueJob, err error) {
	if _, err := jq.q.UpdateJobFailed(ctx, database.UpdateJobFailedParams{
		JobID: job.JobID,
		Error: pgtype.Text{String: err.Error(), Valid: true},
	}); err != nil {
		jq.logger.Error("failed to update job status to 'failed'", "job_id", job.JobID, "error", err)
	}
}

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

type jobQueueConfig struct {
	queueName      string
	logger         *slog.Logger
	pollInterval   time.Duration
	baseRetryDelay time.Duration
	maxRetryDelay  time.Duration
}

type JobQueueOption func(*jobQueueConfig)

func WithLogger(logger *slog.Logger) JobQueueOption {
	return func(jqc *jobQueueConfig) {
		jqc.logger = logger
	}
}

func WithPollInterval(interval time.Duration) JobQueueOption {
	return func(jqc *jobQueueConfig) {
		jqc.pollInterval = interval
	}
}

func WithBaseRetryDelay(d time.Duration) JobQueueOption {
	return func(jqc *jobQueueConfig) {
		jqc.baseRetryDelay = d
	}
}

func WithMaxRetryDelay(d time.Duration) JobQueueOption {
	return func(jqc *jobQueueConfig) {
		jqc.maxRetryDelay = d
	}
}

func WithQueueName(name string) JobQueueOption {
	return func(jqc *jobQueueConfig) {
		jqc.queueName = name
	}
}

type enqueueConfig struct {
	maxRetries  int32
	retryPolicy database.GoqueueRetryPolicy
}

type EnqueueOption func(*enqueueConfig)

func WithMaxRetries(n int32) EnqueueOption {
	return func(ec *enqueueConfig) {
		ec.maxRetries = n
	}
}

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

type RetryPolicy int

const (
	RetryPolicyConstant RetryPolicy = iota
	RetryPolicyLinear
	RetryPolicyExponential
)
