package integration

import (
	"context"
	"testing"

	"github.com/flohansen/goqueue"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var (
	pgContainer *postgres.PostgresContainer
	dbPool      *pgxpool.Pool
)

func TestIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}

var _ = BeforeSuite(func(ctx SpecContext) {
	var err error
	pgContainer, err = postgres.Run(ctx, "postgres:17-alpine", postgres.BasicWaitStrategies())
	Expect(err).NotTo(HaveOccurred())

	connStr, err := pgContainer.ConnectionString(ctx)
	Expect(err).NotTo(HaveOccurred())

	dbPool, err = pgxpool.New(ctx, connStr)
	Expect(err).NotTo(HaveOccurred())

	Expect(goqueue.MigrateUp(dbPool)).To(Succeed())
})

var _ = AfterSuite(func() {
	if pgContainer != nil {
		pgContainer.Terminate(context.Background())
	}
})
