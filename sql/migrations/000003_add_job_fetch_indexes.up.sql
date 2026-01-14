DROP INDEX IF EXISTS idx_goqueue_jobs_status;
DROP INDEX IF EXISTS idx_goqueue_jobs_queue_name;

CREATE INDEX idx_goqueue_jobs_fetch
ON goqueue_jobs(queue_name, status, scheduled_at, created_at)
WHERE status = 'available';