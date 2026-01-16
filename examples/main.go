package main

import (
	"context"
	"log/slog"
	"math/rand"
	"os"
	"time"

	"github.com/flohansen/goqueue"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"golang.org/x/sync/errgroup"
)

// This example demonstrates enqueuing a bunch of jobs and starting a worker
// group processing them concurrently. A PostgreSQL backend is used by spawning
// a temporary Docker container. Here both the enqueing and dequeing
// (processing) is done in a single process. Of course, this can be split into
// two processes, one enqueing jobs, the other processing them.
func main() {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Start a new postgres (backend) container for this example.
	pgContainer, err := postgres.Run(ctx, "postgres:15-alpine",
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Error("failed to start postgres container", "error", err)
		os.Exit(1)
	}

	// Retrieve the connection string for connecting to the postgres container.
	connStr, err := pgContainer.ConnectionString(ctx)
	if err != nil {
		log.Error("failed to get postgres connection string", "error", err)
		os.Exit(1)
	}

	// Setup database connectino pool used for the job queue.
	db, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Error("failed to create connection pool", "error", err)
		os.Exit(1)
	}

	// Migrate the goqueue schema.
	if err := goqueue.MigrateUp(db); err != nil {
		log.Error("failed to run goqueue migrations", "error", err)
		os.Exit(1)
	}

	// Create a queue client processing jobs concurrently. The order of job
	// execution is not guaranteed.
	queue := goqueue.New(db, &testWorker{logger: log},
		goqueue.WithLogger(log),
		goqueue.WithQueueName("uuids"),
		// Uncomment to enable strict First-In-First-Out processing.
		// goqueue.WithFIFO(true),
		goqueue.WithPollInterval(100*time.Millisecond))

	// Create a context for enqueue and processing loops with a maximum runtime
	// of 10s for this example. An error group is being used to stop all
	// concurrent running loops if one fails.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	// Start the enqueue loop. For simulation purpose only schedule 10 jobs.
	g.Go(func() error {
		for range 10 {
			select {
			case <-ctx.Done():
				return nil
			default:
				if _, err := queue.Enqueue(ctx, testArgs{
					Value: []byte(uuid.NewString()),
				}); err != nil {
					return err
				}
			}
		}

		return nil
	})

	// Start the job processing loops simulating 5 workers.
	for range 5 {
		g.Go(func() error {
			queue.Receive(ctx)
			return nil
		})
	}

	// Wait for the enqueue and worker loops to finish and handle errors.
	if err := g.Wait(); err != nil {
		log.Error("application error", "error", err)
		os.Exit(1)
	}

	log.Info("shutdown complete")
}

type testArgs struct {
	Value []byte
}

type testWorker struct {
	logger *slog.Logger
}

func (w *testWorker) Work(ctx context.Context, job *goqueue.Job[testArgs]) error {
	// Simulate work by waiting for 0 - 200ms.
	delay := time.Duration(rand.Intn(200)) * time.Millisecond
	time.Sleep(delay)

	w.logger.Info("processed job", "job_id", job.ID, "value", string(job.Args.Value))
	return nil
}
