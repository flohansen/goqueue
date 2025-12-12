package goqueue

import (
	"errors"
	"fmt"

	"github.com/flohansen/goqueue/sql"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

func MigrateUp(pool *pgxpool.Pool) error {
	sourceDriver, err := iofs.New(sql.MigrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("create iofs driver: %w", err)
	}

	db := stdlib.OpenDBFromPool(pool)
	databaseDriver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("create database driver: %w", err)
	}

	migrator, err := migrate.NewWithInstance("iofs", sourceDriver, "postgres", databaseDriver)
	if err != nil {
		return fmt.Errorf("create migrator: %w", err)
	}

	if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migration error: %w", err)
	}

	return nil
}
