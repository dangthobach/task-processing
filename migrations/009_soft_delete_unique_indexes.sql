-- Soft-deleted configuration names may be reused. Keep uniqueness only among
-- active records so restore remains an explicit conflict-checked operation.
ALTER TABLE projects DROP CONSTRAINT IF EXISTS projects_tenant_id_key_key;
ALTER TABLE retry_policies DROP CONSTRAINT IF EXISTS retry_policies_project_id_name_key;
ALTER TABLE queues DROP CONSTRAINT IF EXISTS queues_project_id_name_key;
ALTER TABLE function_definitions DROP CONSTRAINT IF EXISTS function_definitions_project_id_function_key_version_key;
ALTER TABLE job_definitions DROP CONSTRAINT IF EXISTS job_definitions_project_id_name_key;

CREATE UNIQUE INDEX IF NOT EXISTS ux_projects_active_key ON projects(tenant_id,key) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_retry_policies_active_name ON retry_policies(project_id,name) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_queues_active_name ON queues(project_id,name) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_functions_active_key_version ON function_definitions(project_id,function_key,version) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_job_definitions_active_name ON job_definitions(project_id,name) WHERE deleted_at IS NULL;
