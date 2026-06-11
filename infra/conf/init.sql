
-- =============================================================================
-- DB init script
-- Runs once on first container start via /docker-entrypoint-initdb.d/
-- Superuser at this point is the POSTGRES_USER value from .env.
-- =============================================================================

SET app.nsapp_password TO 'placeholder';  -- overridden by PGOPTIONS at runtime

-- MLflow metadata database
CREATE DATABASE mlflow
    ENCODING 'UTF8'
    LC_COLLATE 'en_US.utf8'
    LC_CTYPE 'en_US.utf8'
    TEMPLATE template0;

GRANT ALL PRIVILEGES ON DATABASE mlflow TO nueralsentinel;

-- Least-privilege application user
-- Password is injected by the entrypoint via NSAPP_PASSWORD env var.
DO $$
BEGIN
  EXECUTE format('CREATE USER nsapp WITH PASSWORD %L', current_setting('app.nsapp_password'));
END
$$;

-- Grant nsapp access to the main app database only
GRANT CONNECT ON DATABASE projects TO nsapp;

\c projects

GRANT USAGE ON SCHEMA public TO nsapp;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO nsapp;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO nsapp;
