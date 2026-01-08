//go:build integration

package integration

import (
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"context"

	"github.com/flohansen/goqueue"
	"github.com/flohansen/goqueue/internal/database"
)

var _ = Describe("Job Queue Integration", func() {
	var (
		q *goqueue.JobQueue[testArgs]
		w *testWorker
	)

	BeforeEach(func(ctx SpecContext) {
		_, err := dbPool.Exec(ctx, "TRUNCATE goqueue_jobs")
		Expect(err).NotTo(HaveOccurred())

		w = &testWorker{}
		q = goqueue.New(dbPool, w, goqueue.WithReconciliationInterval(time.Second))
	})

	Describe("Enqueuing and processing jobs", func() {
		Context("when a job is enqueued", func() {
			var job *goqueue.Job[testArgs]

			BeforeEach(func(ctx SpecContext) {
				var err error
				job, err = q.Enqueue(ctx, testArgs{Foo: "bar"})
				Expect(err).NotTo(HaveOccurred())
			})

			It("should be processed and marked as finished", func(ctx SpecContext) {
				go q.Receive(ctx)

				Eventually(func() string {
					row := dbPool.QueryRow(ctx, "SELECT status FROM goqueue_jobs WHERE job_id = $1", job.ID)
					var status string
					Expect(row.Scan(&status)).To(Succeed())
					return status
				}, "1m", "100ms").Should(Equal("finished"))
			})

			It("should be marked as failed", func(ctx SpecContext) {
				w.ShouldFail = true
				defer func() { w.ShouldFail = false }()
				go q.Receive(ctx)

				Eventually(func() string {
					row := dbPool.QueryRow(ctx, "SELECT status FROM goqueue_jobs WHERE job_id = $1", job.ID)
					var status string
					Expect(row.Scan(&status)).To(Succeed())
					return status
				}, "1m", "100ms").Should(Equal("failed"))
			})

			It("should be rescheduled", func(ctx SpecContext) {
				w.ShouldFail = true
				defer func() { w.ShouldFail = false }()
				go q.Receive(ctx)

				Eventually(func() string {
					row := dbPool.QueryRow(ctx, "SELECT status FROM goqueue_jobs WHERE job_id = $1", job.ID)
					var status string
					Expect(row.Scan(&status)).To(Succeed())
					return status
				}, "1m", "100ms").Should(Equal("failed"))

				w.ShouldFail = false

				Eventually(func() string {
					row := dbPool.QueryRow(ctx, "SELECT status FROM goqueue_jobs WHERE job_id = $1", job.ID)
					var status string
					Expect(row.Scan(&status)).To(Succeed())
					return status
				}, "1m", "100ms").Should(Equal("finished"))
			})

			It("should stay failed after maximum number of retries", func(ctx SpecContext) {
				w.ShouldFail = true
				defer func() { w.ShouldFail = false }()
				go q.Receive(ctx)

				Eventually(func() database.GoqueueJob {
					row := dbPool.QueryRow(ctx, "SELECT status, max_retries, retry_attempt FROM goqueue_jobs WHERE job_id = $1", job.ID)
					var job database.GoqueueJob
					Expect(row.Scan(
						&job.Status,
						&job.MaxRetries,
						&job.RetryAttempt,
					)).To(Succeed())
					return job
				}, "1m", "100ms").Should(Equal(database.GoqueueJob{Status: "failed", MaxRetries: 3, RetryAttempt: 3}))
			})
		})
	})
})

type testArgs struct {
	Foo string `json:"foo"`
}

type testWorker struct {
	goqueue.Worker[testArgs]

	ShouldFail bool
}

func (w *testWorker) Work(ctx context.Context, job *goqueue.Job[testArgs]) error {
	if w.ShouldFail {
		return errors.New("worker failed")
	}

	return nil
}
