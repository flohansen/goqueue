CREATE INDEX idx_goqueue_jobs_status ON goqueue_jobs(status);
CREATE INDEX idx_goqueue_jobs_queue_name ON goqueue_jobs(queue_name);

DROP INDEX IF EXISTS idx_goqueue_jobs_fetch;