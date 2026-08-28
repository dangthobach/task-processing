export type Role = 'admin' | 'operator' | 'developer' | 'viewer';
export type Status =
  | 'ENQUEUE_PENDING'
  | 'QUEUED'
  | 'RESERVED'
  | 'RUNNING'
  | 'RETRY_WAIT'
  | 'SUCCEEDED'
  | 'DEAD_LETTER'
  | 'CANCELLED'
  | 'TIMED_OUT'
  | string;
export type Run = {
  id: string;
  status: Status;
  priority: number;
  version: number;
  available_at: string;
  created_at: string;
  started_at?: string;
  finished_at?: string;
  payload?: unknown;
};
export type Attempt = {
  id: string;
  number: number;
  status: string;
  progress_pct: number;
  progress_message?: string;
  error_class?: string;
  error_code?: string;
  error_message?: string;
  started_at: string;
  finished_at?: string;
};
export type Batch = {
  id: string;
  status: string;
  total_items: number;
  processed_items: number;
  succeeded_items: number;
  failed_items: number;
  retry_scheduled_items: number;
  created_at: string;
  started_at?: string;
  finished_at?: string;
  progress_pct?: number;
};
export type BatchItem = {
  id: string;
  job_run_id: string;
  ordinal: number;
  status: string;
  error_class?: string;
  error_code?: string;
  error_message?: string;
  completed_at?: string;
};
export type BatchAttempt = {
  id: string;
  attempt_number: number;
  status: string;
  total_items: number;
  processed_items: number;
  succeeded_items: number;
  failed_items: number;
  progress_pct: number;
  progress_message?: string;
  started_at: string;
  finished_at?: string;
};
export type BatchLog = {
  id: number;
  batch_attempt_id?: string;
  level: string;
  message: string;
  fields: unknown;
  created_at: string;
};
export type Dlq = { id: string; job_run_id: string; reason: string; version: number; entered_at: string };
export type Worker = {
  id: string;
  worker_key: string;
  hostname: string;
  version: string;
  status: string;
  heartbeat_at: string;
};
export type SchedulerLog = {
  id: string;
  schedule_id?: string;
  scheduler_instance_id: string;
  level: string;
  event_type: string;
  message: string;
  details: unknown;
  occurred_at: string;
};
export type Audit = {
  id: string;
  actor_id: string;
  action: string;
  resource_type: string;
  resource_id: string;
  created_at: string;
};
export type PlatformAudit = Audit & {
  tenant_id?: string;
  before_data?: unknown;
  after_data?: unknown;
  request_id?: string;
  trace_id?: string;
  metadata?: unknown;
};
export type RealtimeEvent = {
  id: number;
  event_type: string;
  aggregate_type: string;
  aggregate_id: string;
  payload: unknown;
  created_at: string;
};
