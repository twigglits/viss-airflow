-- Create an additional database `viss` for raster objects if it does not already exist
DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_database WHERE datname = 'viss'
   ) THEN
      EXECUTE 'CREATE DATABASE viss OWNER airflow';
   END IF;
END
$$;
