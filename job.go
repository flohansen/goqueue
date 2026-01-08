package goqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
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
	db     DB
	q      database.Querier
	worker Worker[T]
	logger *slog.Logger
}

func New[T any](db DB, worker Worker[T]) *JobQueue[T] {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	return &JobQueue[T]{
		db:     db,
		q:      database.New(db),
		worker: worker,
		logger: logger,
	}
}

type Job[T any] struct {
	ID   int32
	Args T
}

func (jq *JobQueue[T]) Enqueue(ctx context.Context, args T) (*Job[T], error) {
	b, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("failed to json encode job arguments: %w", err)
	}

	job, err := jq.q.InsertJob(ctx, b)
	if err != nil {
		return nil, fmt.Errorf("failed to insert job: %w", err)
	}

	return &Job[T]{
		ID:   job.JobID,
		Args: args,
	}, nil
}

func (jq *JobQueue[T]) Receive(ctx context.Context) {
	go func() {
		for {
			time.Sleep(time.Second)
			if err := jq.reconcile(ctx); err != nil {
				jq.logger.Error("reconciliation failed", "error", err)
				continue
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
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
	job, err := jq.q.FetchJobLocked(ctx)
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
		jq.markJobFailed(ctx, job, err)
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

func (jq *JobQueue[T]) markJobFailed(ctx context.Context, job database.GoqueueJob, err error) {
	nextRetryTime := jq.calculateNextRetry(job)
	if _, err := jq.q.UpdateJobFailed(ctx, database.UpdateJobFailedParams{
		JobID:       job.JobID,
		Error:       pgtype.Text{String: err.Error(), Valid: true},
		NextRetryAt: pgtype.Timestamp{Time: nextRetryTime.UTC(), Valid: true},
	}); err != nil {
		jq.logger.Error("failed to update job status to 'failed'", "jobId", job.JobID, "error", err)
	}
}

func (jq *JobQueue[T]) calculateNextRetry(job database.GoqueueJob) time.Time {
	delay := time.Duration(math.Pow(2.0, float64(job.RetryAttempt+1))) * time.Second
	return time.Now().Add(delay)
}

func (jq *JobQueue[T]) reconcile(ctx context.Context) error {
	tx, err := jq.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	dtx := database.New(jq.db).WithTx(tx)

	jobs, err := dtx.FetchRescheduableJobsLocked(ctx, 100)
	if err != nil {
		return fmt.Errorf("failed to get rescheduable jobs: %w", err)
	}

	for _, job := range jobs {
		if err := jq.rescheduleJob(ctx, dtx, job); err != nil {
			return fmt.Errorf("failed to reschedule job batch: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (jq *JobQueue[T]) rescheduleJob(ctx context.Context, q database.Querier, job database.GoqueueJob) error {
	if job.RetryAttempt >= job.MaxRetries {
		return nil
	}

	_, err := q.RescheduleJob(ctx, database.RescheduleJobParams{
		JobID:        job.JobID,
		RetryAttempt: job.RetryAttempt + 1,
	})
	if err != nil {
		return fmt.Errorf("failed to reschedule job with id %d: %w", job.JobID, err)
	}

	return nil
}
