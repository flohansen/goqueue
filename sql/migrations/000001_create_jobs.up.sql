CREATE TYPE goqueue_retry_policy AS ENUM (
  'constant',
  'linear',
  'exponential'
);

CREATE TYPE goqueue_job_status AS ENUM (
  'available',
  'pending',
  'failed',
  'finished'
);

CREATE TABLE IF NOT EXISTS goqueue_jobs (
  job_id SERIAL PRIMARY KEY,
  queue_name TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  scheduled_at TIMESTAMP,
  max_retries INTEGER NOT NULL DEFAULT 3,
  retry_attempt INTEGER NOT NULL DEFAULT 0,
  retry_policy goqueue_retry_policy NOT NULL DEFAULT 'exponential',
  status goqueue_job_status NOT NULL,
  error TEXT,
  arguments JSONB NOT NULL
);

CREATE INDEX idx_goqueue_jobs_status ON goqueue_jobs(status);
CREATE INDEX idx_goqueue_jobs_queue_name ON goqueue_jobs(queue_name);