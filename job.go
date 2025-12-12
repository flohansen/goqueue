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

type JobQueue[T any] struct {
	db     database.Querier
	worker Worker[T]
	logger *slog.Logger
}

type Worker[T any] interface {
	Work(ctx context.Context, job *Job[T]) error
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
		Status:    "available",
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
				var t *internalerrs.ErrNoJob
				if errors.As(err, &t) {
					continue
				}

				q.logger.Error("receive error", "error", err)
			}
		}
	}
}

func (q *JobQueue[T]) receive(ctx context.Context) error {
	job, err := q.db.GetJob(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &internalerrs.ErrNoJob{}
		}

		return &internalerrs.ErrReceiveJob{Err: err}
	}

	var args T
	if err := json.Unmarshal(job.Arguments, &args); err != nil {
		return &internalerrs.ErrReceiveJob{Err: err}
	}

	job, err = q.db.UpdateJob(ctx, database.UpdateJobParams{
		JobID:      job.JobID,
		CreatedAt:  job.CreatedAt,
		FinishedAt: job.FinishedAt,
		Status:     "pending",
		Error:      job.Error,
		Arguments:  job.Arguments,
	})
	if err != nil {
		return &internalerrs.ErrUpdateJobState{
			CurrentState: job.Status,
			WantedState:  "pending",
			Err:          err,
		}
	}

	if err := q.worker.Work(ctx, &Job[T]{
		Args: args,
	}); err != nil {
		return &internalerrs.ErrWorkerFailed{Err: err}
	}

	if _, err := q.db.UpdateJob(ctx, database.UpdateJobParams{
		JobID:      job.JobID,
		CreatedAt:  job.CreatedAt,
		FinishedAt: pgtype.Timestamp{Time: time.Now(), Valid: true},
		Status:     "finished",
		Error:      job.Error,
		Arguments:  job.Arguments,
	}); err != nil {
		return &internalerrs.ErrUpdateJobState{
			CurrentState: job.Status,
			WantedState:  "finished",
			Err:          err,
		}
	}

	return nil
}
