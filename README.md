# goqueue
![license](https://img.shields.io/github/license/flohansen/goqueue)

goqueue is a simple and efficient queue implementation in Go.

## Table of Contents
- [Features](#features)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Features
- Concurrent processing through worker jobs
- Retry mechanism with customizable retry logic
- PostgreSQL backend for reliable job storage
- Structured logging support with slog

## Usage
First, define your job arguments and worker by implementing the `goqueue.Worker` interface:

```go
type myArgs struct {
    // Define your job arguments here
    Foo string `json:"foo"`
}

type myWorker struct {}

func (w *myWorker) Work(ctx context.Context, job *goqueue.Job[myArgs]) error {
    // Implement your job processing logic here
    fmt.Printf("Foo: %s\n", job.Args.Foo)
    return nil
}
```

Next, create a logger, initialize the queue, enqueue jobs, and start receiving jobs:

```go
log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

pool, err := pgxpool.New(ctx, "postgresql://user:password@localhost:5432/mydb")
if err != nil {
    log.Error("failed to create pgx pool", "error", err)
    os.Exit(1)
}

queue := goqueue.New(pool, &myWorker{},
    goqueue.WithQueueName("my-queue"),
    goqueue.WithLogger(log))
```

On producer side, enqueue jobs:

```go
job, err := queue.Enqueue(ctx, myArgs{Foo: "bar"})
```

On consumer side, receive and process jobs:

```go
err := queue.Receive(ctx)
```

That's it! You now have a simple queue system set up in Go using `goqueue`.

## Contributing
Contributions are welcome! Please open an issue or submit a pull request.

## License
This project is licensed under the GPL-3.0 License. See the [LICENSE](LICENSE) file for details.
