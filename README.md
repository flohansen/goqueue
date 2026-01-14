# goqueue

![License](https://img.shields.io/github/license/flohansen/goqueue)
[![Go Report Card](https://goreportcard.com/badge/github.com/flohansen/goqueue)](https://goreportcard.com/report/github.com/flohansen/goqueue)

A simple, efficient, and reliable queue implementation for Go with PostgreSQL backend support.

## Table of Contents
- [Features](#features)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Development](#development)
  - [Available Commands](#available-commands)
  - [Testing](#testing)
  - [Working with SQL](#working-with-sql)
- [Using Nix (Optional)](#using-nix-optional)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Concurrent Processing**: Efficient worker-based job processing
- **FIFO Queues**: First-In-First-Out job queues for strict ordering requirements
- **Retry Mechanism**: Customizable retry logic for failed jobs
- **PostgreSQL Backend**: Reliable, persistent job storage
- **Structured Logging**: Built-in support for Go's `slog` package

## Getting Started

### Prerequisites

- [Go](https://go.dev) 1.21 or higher
- [PostgreSQL](https://www.postgresql.org/) 13 or higher
- [sqlc](https://sqlc.dev) for generating type-safe SQL bindings
- [Make](https://www.gnu.org/software/make/) for build automation

### Installation
```bash
git clone https://github.com/flohansen/goqueue
cd goqueue
```

## Development

### Available Commands

| Command | Description |
|---------|-------------|
| `make generate` | Generate Go code from SQL files (required after any `.sql` file changes) |
| `make test` | Run all unit tests |
| `make test-it` | Run all integration tests |

### Testing

#### Unit Tests

Unit tests follow Go conventions and are located alongside implementation files:
```
.
├── queue.go
├── queue_test.go
└── ...
```

Run unit tests:
```bash
make test
```

Or manually:
```bash
go test ./... -cover -v
```

#### Integration Tests

Integration tests are located in `tests/integration/` and require the `integration` build tag:
```go
//go:build integration
```

Run integration tests:
```bash
make test-it
```

Or manually:
```bash
go test -tags=integration ./test/integration/... -v
```

### Working with SQL

#### Migrations

SQL migration files are stored in `sql/migrations/` and follow this naming convention:
```
<version>_<name>.[up|down].sql
```

Example: `000001_create_jobs_table.up.sql`

#### Queries

SQL query files are stored in `sql/queries/`. Go bindings are automatically generated from these files using sqlc.

**Important**: After modifying any `.sql` file, regenerate the Go bindings:
```bash
make generate
```

## Using Nix (Optional)

This repository includes a `flake.nix` for simplified dependency management. With Nix installed, you can:

Spawn a development shell with all dependencies:
```bash
nix develop
```

Or run commands directly:
```bash
nix develop --command make generate
```

## Contributing

Contributions are welcome! Here's how you can help:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes and add tests
4. Run tests to ensure everything works (`make test && make test-it`)
5. Commit your changes (`git commit -m 'feat: add amazing feature'`)
6. Push to your branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

Please ensure your code follows Go best practices and includes appropriate tests.

## License

This project is licensed under the GPL-3.0 License. See the [LICENSE](LICENSE) file for details.

---

**Questions or Issues?** Feel free to open an issue on [GitHub](https://github.com/flohansen/goqueue/issues).
