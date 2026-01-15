//go:build integration

package integration

import (
	"bytes"
	"errors"
	"log/slog"
	"math/rand"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"context"

	"github.com/flohansen/goqueue"
	"github.com/flohansen/goqueue/internal/database"
)

var _ = Describe("FIFO Queue Integration", func() {
	var (
		logBuf bytes.Buffer
		q      *goqueue.JobQueue[testArgs]
		w      *testWorker
	)

	BeforeEach(func(ctx SpecContext) {
		_, err := dbPool.Exec(ctx, "TRUNCATE goqueue_jobs")
		Expect(err).NotTo(HaveOccurred())
		_, err = dbPool.Exec(ctx, "TRUNCATE goqueue_queues")
		Expect(err).NotTo(HaveOccurred())

		logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		logBuf.Reset()

		w = &testWorker{}
		q = goqueue.New(dbPool, w,
			goqueue.WithFIFO(true),
			goqueue.WithLogger(logger))
	})

	Describe("Enqueuing and processing jobs", func() {
		Context("when multiple jobs are enqueued", func() {
			var jobs []*goqueue.Job[testArgs]

			BeforeEach(func(ctx SpecContext) {
				jobs = []*goqueue.Job[testArgs]{}
				for range 10 {
					job, err := q.Enqueue(ctx, testArgs{Foo: "bar"})
					Expect(err).NotTo(HaveOccurred())
					jobs = append(jobs, job)
				}
			})

			It("should use index", func(ctx SpecContext) {
				for range 3 {
					go q.Receive(ctx)
				}

				Eventually(func() int {
					row := dbPool.QueryRow(ctx, "SELECT COUNT(*) FROM goqueue_jobs WHERE status = 'finished'")
					var count int
					Expect(row.Scan(&count)).To(Succeed())
					return count
				}, "1m", "100ms").Should(BeNumerically(">", 0))

				scans := getIndexScans(dbPool, "idx_goqueue_jobs_fetch")
				Expect(scans).To(BeNumerically(">", 0), "Index should have been used during job fetching/processing")
			})

			It("should be processed and marked as finished", func(ctx SpecContext) {
				for range 3 {
					go q.Receive(ctx)
				}

				Eventually(func() int {
					row := dbPool.QueryRow(ctx, "SELECT COUNT(*) FROM goqueue_jobs WHERE status = 'finished'")
					var count int
					Expect(row.Scan(&count)).To(Succeed())
					return count
				}, "1m", "100ms").Should(Equal(len(jobs)))

				Expect(w.JobsProcessed).To(Equal(jobs))
			})

		})
	})
})

var _ = Describe("Job Queue Integration", func() {
	var (
		logBuf bytes.Buffer
		q      *goqueue.JobQueue[testArgs]
		w      *testWorker
	)

	BeforeEach(func(ctx SpecContext) {
		_, err := dbPool.Exec(ctx, "TRUNCATE goqueue_jobs")
		Expect(err).NotTo(HaveOccurred())
		_, err = dbPool.Exec(ctx, "TRUNCATE goqueue_queues")
		Expect(err).NotTo(HaveOccurred())

		logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		logBuf.Reset()

		w = &testWorker{}
		q = goqueue.New(dbPool, w, goqueue.WithLogger(logger))
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

				Expect(w.JobsProcessed).To(Equal([]*goqueue.Job[testArgs]{job}))
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

				Eventually(func() database.GoqueueJob {
					row := dbPool.QueryRow(ctx, "SELECT status, max_retries, retry_attempt FROM goqueue_jobs WHERE job_id = $1", job.ID)
					var job database.GoqueueJob
					Expect(row.Scan(
						&job.Status,
						&job.MaxRetries,
						&job.RetryAttempt,
					)).To(Succeed())
					return job
				}, "1m", "100ms").Should(Equal(database.GoqueueJob{Status: "available", MaxRetries: 3, RetryAttempt: 1}))

				w.ShouldFail = false

				Eventually(func() database.GoqueueJob {
					row := dbPool.QueryRow(ctx, "SELECT status, max_retries, retry_attempt FROM goqueue_jobs WHERE job_id = $1", job.ID)
					var job database.GoqueueJob
					Expect(row.Scan(
						&job.Status,
						&job.MaxRetries,
						&job.RetryAttempt,
					)).To(Succeed())
					return job
				}, "1m", "100ms").Should(Equal(database.GoqueueJob{Status: "finished", MaxRetries: 3, RetryAttempt: 2}))

				Expect(w.JobsProcessed).To(Equal([]*goqueue.Job[testArgs]{job}))
			})

			It("should stay failed after maximum number of retries", func(ctx SpecContext) {
				w.ShouldFail = true
				defer func() { w.ShouldFail = false }()
				go q.Receive(ctx)

				Eventually(func() database.GoqueueJob {
					row := dbPool.QueryRow(ctx, "SELECT status, max_retries, retry_attempt, error FROM goqueue_jobs WHERE job_id = $1", job.ID)
					var job database.GoqueueJob
					Expect(row.Scan(
						&job.Status,
						&job.MaxRetries,
						&job.RetryAttempt,
						&job.Error,
					)).To(Succeed())
					return job
				}, "1m", "100ms").Should(Equal(database.GoqueueJob{Status: "failed", MaxRetries: 3, RetryAttempt: 3, Error: pgtype.Text{String: "maximum number of retries reached", Valid: true}}))
			})
		})

		Context("when multiple jobs are enqueued", func() {
			var jobs []*goqueue.Job[testArgs]

			BeforeEach(func(ctx SpecContext) {
				jobs = []*goqueue.Job[testArgs]{}
				for range 10 {
					job, err := q.Enqueue(ctx, testArgs{Foo: "bar"})
					Expect(err).NotTo(HaveOccurred())
					jobs = append(jobs, job)
				}
			})

			Context("and one receiver is running", func() {
				It("should process all jobs", func(ctx SpecContext) {
					go q.Receive(ctx)

					Eventually(func() int {
						row := dbPool.QueryRow(ctx, "SELECT COUNT(*) FROM goqueue_jobs WHERE status = 'finished'")
						var count int
						Expect(row.Scan(&count)).To(Succeed())
						return count
					}, "1m", "100ms").Should(Equal(len(jobs)))

					slices.SortFunc(w.JobsProcessed, func(a, b *goqueue.Job[testArgs]) int {
						return int(a.ID - b.ID)
					})
					Expect(w.JobsProcessed).To(Equal(jobs))
				})
			})

			Context("and multiple receivers are running", func() {
				It("should process all jobs without duplication", func(ctx SpecContext) {
					for range 3 {
						go q.Receive(ctx)
					}

					Eventually(func() int {
						row := dbPool.QueryRow(ctx, "SELECT COUNT(*) FROM goqueue_jobs WHERE status = 'finished'")
						var count int
						Expect(row.Scan(&count)).To(Succeed())
						return count
					}, "1m", "100ms").Should(Equal(len(jobs)))

					slices.SortFunc(w.JobsProcessed, func(a, b *goqueue.Job[testArgs]) int {
						return int(a.ID - b.ID)
					})
					Expect(w.JobsProcessed).To(Equal(jobs))
				})
			})
		})
	})
})

type testArgs struct {
	Foo string `json:"foo"`
}

type testWorker struct {
	goqueue.Worker[testArgs]

	ShouldFail    bool
	JobsProcessed []*goqueue.Job[testArgs]
}

func (w *testWorker) Work(ctx context.Context, job *goqueue.Job[testArgs]) error {
	if w.ShouldFail {
		return errors.New("worker failed")
	}

	time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
	w.JobsProcessed = append(w.JobsProcessed, job)
	return nil
}

func getIndexScans(dbPool *pgxpool.Pool, name string) int64 {
	ctx := context.Background()

	query := `
	SELECT COALESCE(idx_scan, 0)
	FROM pg_stat_user_indexes
	WHERE indexrelname = $1
	`

	var scans int64
	err := dbPool.QueryRow(ctx, query, name).Scan(&scans)
	if err != nil {
		GinkgoWriter.Printf("%v\n", err)
		return 0
	}

	return scans
}
