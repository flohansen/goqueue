package sql

import "embed"

var (
	//go:embed migrations
	MigrationsFS embed.FS
)
