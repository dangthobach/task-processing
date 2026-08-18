ALTER TABLE tenants ADD COLUMN IF NOT EXISTS max_concurrency integer NOT NULL DEFAULT 20 CHECK(max_concurrency > 0);
ALTER TABLE projects ADD COLUMN IF NOT EXISTS max_concurrency integer NOT NULL DEFAULT 20 CHECK(max_concurrency > 0);
ALTER TABLE function_definitions ADD COLUMN IF NOT EXISTS max_concurrency integer NOT NULL DEFAULT 5 CHECK(max_concurrency > 0);
