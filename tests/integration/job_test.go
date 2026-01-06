//go:build integration

package integration

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"context"

	"github.com/flohansen/goqueue"
	"github.com/flohansen/goqueue/internal/database"
)

var _ = Describe("Job Queue Integration", func() {
	var (
		q *goqueue.JobQueue[testArgs]
	)

	BeforeEach(func(ctx SpecContext) {
		_, err := dbPool.Exec(ctx, "TRUNCATE goqueue_jobs")
		Expect(err).NotTo(HaveOccurred())

		q = goqueue.New(database.New(dbPool), &testWorker{})
	})

	Describe("Enqueuing and processing jobs", func() {
		Context("when a job is enqueued", func() {
			var job *goqueue.Job[testArgs]

			BeforeEach(func(ctx SpecContext) {
				var err error
				job, err = q.Send(ctx, testArgs{
					Foo: "bar",
				})
				Expect(err).NotTo(HaveOccurred())
			})

			It("should be processed successfully", func(ctx SpecContext) {
				go q.Receive(ctx)

				Eventually(func() string {
					row := dbPool.QueryRow(ctx, "SELECT status FROM goqueue_jobs WHERE job_id = $1", job.ID)
					var status string
					Expect(row.Scan(&status)).To(Succeed())
					return status
				}, "1m", "100ms").Should(Equal("finished"))
			})
		})
	})
})

type testArgs struct {
	Foo string `json:"foo"`
}

type testWorker struct {
	goqueue.Worker[testArgs]
}

func (w *testWorker) Work(ctx context.Context, job *goqueue.Job[testArgs]) error {
	return nil
}
