CREATE TABLE IF NOT EXISTS jobs (
  job_id SERIAL PRIMARY KEY,
  created_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP,
  status TEXT NOT NULL,
  error TEXT,
  arguments JSONB NOT NULL
);
