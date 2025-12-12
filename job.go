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
			job, err := q.db.GetJob(ctx)
			if err != nil {
				if !errors.Is(err, pgx.ErrNoRows) {
					q.logger.Error("failed to get job", "error", err)
				}
				continue
			}

			var args T
			if err := json.Unmarshal(job.Arguments, &args); err != nil {
				q.logger.Error("failed to unmarshal job arguments", "error", err)
				continue
			}

			if _, err := q.db.UpdateJob(ctx, database.UpdateJobParams{
				JobID:      job.JobID,
				CreatedAt:  job.CreatedAt,
				FinishedAt: job.FinishedAt,
				Status:     "pending",
				Error:      job.Error,
				Arguments:  job.Arguments,
			}); err != nil {
				q.logger.Error("failed to update job to pending state", "error", err)
				continue
			}

			if err := q.worker.Work(ctx, &Job[T]{
				Args: args,
			}); err != nil {
				q.logger.Error("worker error", "error", err)
				continue
			}

			if _, err := q.db.UpdateJob(ctx, database.UpdateJobParams{
				JobID:      job.JobID,
				CreatedAt:  job.CreatedAt,
				FinishedAt: pgtype.Timestamp{Time: time.Now(), Valid: true},
				Status:     "finished",
				Error:      job.Error,
				Arguments:  job.Arguments,
			}); err != nil {
				q.logger.Error("failed to update job to finished state", "error", err)
				continue
			}
		}
	}
}
