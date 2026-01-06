-- Tables for storing vrust SEIRS runs and time series (population + infected)

CREATE TABLE IF NOT EXISTS seirs_runs (
  id TEXT PRIMARY KEY,
  iso3 TEXT NOT NULL,
  year INTEGER NOT NULL,
  params JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS seirs_runs_iso3_year_created_at_idx
  ON seirs_runs (iso3, year, created_at DESC);

CREATE TABLE IF NOT EXISTS seirs_series_points (
  run_id TEXT NOT NULL REFERENCES seirs_runs(id) ON DELETE CASCADE,
  t DOUBLE PRECISION NOT NULL,
  population DOUBLE PRECISION NOT NULL,
  infected DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (run_id, t)
);

CREATE INDEX IF NOT EXISTS seirs_series_points_run_idx
  ON seirs_series_points (run_id);
