import type { Attempt, Audit, Batch, BatchAttempt, BatchItem, BatchLog, Dlq, RealtimeEvent, Run, SchedulerLog, Worker } from './types'

export type Connection = { baseUrl:string; actorId:string; tenantId:string; projectId:string; role:string }
export type ControlAggregate = 'queues'|'retry-policies'|'rate-limit-policies'|'retention-policies'|'function-definitions'|'job-definitions'|'schedules'
export type ControlRecord = { id:string; version:number; deleted_at?:string|null; status?:string; name?:string; function_key?:string; [key:string]:unknown }
export class ApiError extends Error { constructor(public status:number, public code?:string, message?:string){ super(message) } }
type Envelope<T> = { data:T; meta:{ request_id:string; trace_id:string; timestamp:string } }

export function createApi(connection:Connection) {
  const query = (path:string) => `${connection.baseUrl.replace(/\/$/, '')}${path}${path.includes('?')?'&':'?'}project_id=${encodeURIComponent(connection.projectId)}`
  const call = async<T>(path:string, init:RequestInit={}) => {
    const response = await fetch(path.startsWith('/api/') ? query(path) : `${connection.baseUrl.replace(/\/$/, '')}${path}`, { ...init, headers:{ 'Content-Type':'application/json', 'X-Actor-ID':connection.actorId, 'X-Tenant-ID':connection.tenantId, 'X-Role':connection.role, ...init.headers } })
    if (!response.ok) { const body=await response.json().catch(()=>null); throw new ApiError(response.status, body?.error?.code, body?.error?.detail || response.statusText) }
    if (response.status===204) return undefined as T
    const body = await response.json() as Envelope<T>
    return body.data
  }
  return {
    health: () => call<{status:string}>('/healthz'),
    runs: () => call<Run[]>('/api/v1/job-runs'),
    run: (id:string) => call<Run>(`/api/v1/job-runs/${id}`),
    attempts: (id:string) => call<Attempt[]>(`/api/v1/job-runs/${id}/attempts`),
    createRun: (definitionId:string, body:{payload:unknown;idempotency_key?:string;priority?:number}) => call<Run>(`/api/v1/job-definitions/${definitionId}/run`,{method:'POST',body:JSON.stringify({project_id:connection.projectId,...body})}),
    bulk: (body:{job_definition_id:string;items:Array<{payload:unknown;idempotency_key?:string;priority?:number}>}) => call<{batch_id:string;runs:Run[]}>('/api/v1/job-runs:bulk',{method:'POST',body:JSON.stringify({project_id:connection.projectId,...body})}),
    cancel: (id:string, version:number) => call(`/api/v1/job-runs/${id}/cancel`,{method:'POST',headers:{'If-Match':`"${version}"`}}), retry: (id:string, version:number) => call(`/api/v1/job-runs/${id}/retry`,{method:'POST',headers:{'If-Match':`"${version}"`}}),
    batches: () => call<Batch[]>('/api/v1/job-batches'), batch:(id:string)=>call<Batch>(`/api/v1/job-batches/${id}`), batchItems:(id:string)=>call<BatchItem[]>(`/api/v1/job-batches/${id}/items`), batchAttempts:(id:string)=>call<BatchAttempt[]>(`/api/v1/job-batches/${id}/attempts`), batchLogs:(id:string)=>call<BatchLog[]>(`/api/v1/job-batches/${id}/logs`),
    dlq:()=>call<Dlq[]>('/api/v1/dlq'), replayDlq:(id:string,version:number)=>call(`/api/v1/dlq/${id}/replay`,{method:'POST',headers:{'If-Match':`"${version}"`}}), workers:()=>call<Worker[]>('/api/v1/workers'), schedulerLogs:()=>call<{items:SchedulerLog[]}>('/api/v1/scheduler-logs'), audits:()=>call<Audit[]>('/api/v1/audit-logs'),
    createQueue:(body:unknown)=>call('/api/v1/queues',{method:'POST',body:JSON.stringify({project_id:connection.projectId,...body as object})}), queueAction:(id:string,action:'pause'|'resume'|'drain',version:number)=>call(`/api/v1/queues/${id}/${action}`,{method:'POST',headers:{'If-Match':`"${version}"`}}),
    createRetryPolicy:(body:unknown)=>call('/api/v1/retry-policies',{method:'POST',body:JSON.stringify({project_id:connection.projectId,...body as object})}), createRateLimitPolicy:(body:unknown)=>call('/api/v1/rate-limit-policies',{method:'POST',body:JSON.stringify({project_id:connection.projectId,...body as object})}), createRetentionPolicy:(body:unknown)=>call('/api/v1/retention-policies',{method:'POST',body:JSON.stringify({project_id:connection.projectId,...body as object})}), createFunction:(body:unknown)=>call('/api/v1/function-definitions',{method:'POST',body:JSON.stringify({project_id:connection.projectId,...body as object})}), createDefinition:(body:unknown)=>call('/api/v1/job-definitions',{method:'POST',body:JSON.stringify({project_id:connection.projectId,...body as object})}), createSchedule:(body:unknown)=>call('/api/v1/schedules',{method:'POST',body:JSON.stringify({project_id:connection.projectId,...body as object})}), previewSchedule:(body:{cron_expression:string;timezone:string;with_seconds?:boolean;from?:string})=>call<{timezone:string;occurrences:string[]}>('/api/v1/schedules/preview',{method:'POST',body:JSON.stringify(body)}), scheduleAction:(id:string,action:'pause'|'resume',version:number)=>call(`/api/v1/schedules/${id}/${action}`,{method:'POST',headers:{'If-Match':`"${version}"`}}),
    workflows:()=>call<Array<{id:string;name:string;status:string;version:number;created_at:string}>>('/api/v1/workflows'), createWorkflow:(body:unknown)=>call<{id:string;version:number}>('/api/v1/workflows',{method:'POST',body:JSON.stringify({project_id:connection.projectId,...body as object})}), startWorkflow:(id:string,input:unknown)=>call<{id:string;status:string}>(`/api/v1/workflows/${id}/runs`,{method:'POST',body:JSON.stringify({input})}),
    control:(aggregate:ControlAggregate,includeDeleted=false)=>call<{items:ControlRecord[]}>(`/api/v1/${aggregate}${includeDeleted?'?include_deleted=true':''}`), controlGet:(aggregate:ControlAggregate,id:string,includeDeleted=false)=>call<ControlRecord>(`/api/v1/${aggregate}/${id}${includeDeleted?'?include_deleted=true':''}`), updateControl:(aggregate:ControlAggregate,id:string,version:number,body:unknown)=>call<ControlRecord>(`/api/v1/${aggregate}/${id}`,{method:'PATCH',headers:{'If-Match':`"${version}"`},body:JSON.stringify(body)}), deleteControl:(aggregate:ControlAggregate,id:string,version:number)=>call<ControlRecord>(`/api/v1/${aggregate}/${id}`,{method:'DELETE',headers:{'If-Match':`"${version}"`}}), restoreControl:(aggregate:ControlAggregate,id:string,version:number)=>call<ControlRecord>(`/api/v1/${aggregate}/${id}/restore`,{method:'POST',headers:{'If-Match':`"${version}"`}}),
    events: (after:number, signal:AbortSignal) => fetch(query(`/api/v1/events?after_id=${after}`), { headers:{'X-Actor-ID':connection.actorId,'X-Tenant-ID':connection.tenantId,'X-Role':connection.role}, signal }),
  }
}
export type Api = ReturnType<typeof createApi>
export async function parseSse(response:Response,onEvent:(event:RealtimeEvent)=>void){ const reader=response.body?.getReader();if(!reader)return;const decoder=new TextDecoder();let buffer='';while(true){const {done,value}=await reader.read();if(done)return;buffer+=decoder.decode(value,{stream:true});const frames=buffer.split('\n\n');buffer=frames.pop()||'';for(const frame of frames){const data=frame.split('\n').find(line=>line.startsWith('data: '))?.slice(6);if(data){try{onEvent(JSON.parse(data))}catch{/* ignore malformed server event */}}}}}
