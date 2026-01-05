package goqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/flohansen/goqueue/internal/database"
	internalerrs "github.com/flohansen/goqueue/internal/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type Worker[T any] interface {
	Work(ctx context.Context, job *Job[T]) error
}

type JobQueue[T any] struct {
	db     database.Querier
	worker Worker[T]
	logger *slog.Logger
}

func New[T any](db database.Querier, worker Worker[T]) *JobQueue[T] {
	return &JobQueue[T]{
		db:     db,
		worker: worker,
		logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
	}
}

type Job[T any] struct {
	Args T
}

func (q *JobQueue[T]) Send(ctx context.Context, args T) error {
	b, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("failed to json encode job arguments: %w", err)
	}

	if _, err := q.db.InsertJob(ctx, database.InsertJobParams{
		CreatedAt: pgtype.Timestamp{Time: time.Now(), Valid: true},
		Status:    database.GoqueueJobStatusAvailable,
		Arguments: b,
	}); err != nil {
		return fmt.Errorf("failed to insert job: %w", err)
	}

	return nil
}

func (q *JobQueue[T]) Receive(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
			if err := q.receive(ctx); err != nil {
				var t *internalerrs.NoJobError
				if errors.As(err, &t) {
					continue
				}

				q.logger.Error("receive error", "error", err)
			}
		}
	}
}

func (q *JobQueue[T]) receive(ctx context.Context) error {
	job, err := q.db.FetchJobLocked(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &internalerrs.NoJobError{}
		}

		return &internalerrs.ReceiveJobError{Err: err}
	}

	var args T
	if err := json.Unmarshal(job.Arguments, &args); err != nil {
		q.markJobFailed(ctx, job, err)
		return &internalerrs.ReceiveJobError{Err: err}
	}

	if err := q.worker.Work(ctx, &Job[T]{
		Args: args,
	}); err != nil {
		q.markJobFailed(ctx, job, err)
		return &internalerrs.WorkerFailedError{Err: err}
	}

	if _, err := q.db.UpdateJobFinished(ctx, job.JobID); err != nil {
		return &internalerrs.UpdateJobStatusError{
			CurrentState: string(job.Status),
			WantedState:  string(database.GoqueueJobStatusFinished),
			Err:          err,
		}
	}

	return nil
}

func (q *JobQueue[T]) markJobFailed(ctx context.Context, job database.GoqueueJob, err error) {
	if _, err := q.db.UpdateJobFailed(ctx, database.UpdateJobFailedParams{
		JobID: job.JobID,
		Error: pgtype.Text{String: err.Error(), Valid: true},
	}); err != nil {
		q.logger.Error("failed to update job status to 'failed'", "jobId", job.JobID, "error", err)
	}
}
