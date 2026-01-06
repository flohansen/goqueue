package main

import (
	"context"
	"log"
	"time"

	"github.com/flohansen/goqueue"
	"github.com/flohansen/goqueue/internal/database"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

type testArgs struct {
	Value []byte
}

func main() {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Fatal(err)
	}

	connStr, err := pgContainer.ConnectionString(ctx)
	if err != nil {
		log.Fatal(err)
	}

	db, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatal(err)
	}

	if err := goqueue.MigrateUp(db); err != nil {
		log.Fatal(err)
	}

	queue := goqueue.New(database.New(db), &testWorker{})
	go func() {
		for range 10 {
			queue.Enqueue(ctx, testArgs{
				Value: []byte(uuid.NewString()),
			})
			time.Sleep(time.Second)
		}
	}()

	queue.Receive(ctx)
}

type testWorker struct {
}

func (*testWorker) Work(ctx context.Context, job *goqueue.Job[testArgs]) error {
	log.Printf("%q\n", string(job.Args.Value))
	return nil
}
