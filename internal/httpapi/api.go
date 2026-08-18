package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/example/task-processing/internal/application/controlplane"
	"github.com/example/task-processing/internal/application/tasks"
	"github.com/example/task-processing/internal/domain/job"
	"github.com/example/task-processing/internal/identity"
	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/example/task-processing/internal/telemetry"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
)

type principalKey struct{}
type requestMetaKey struct{}

// RequestMeta is returned with every JSON API response and carried through the
// request context so audit records, logs and traces share the same correlation IDs.
type RequestMeta struct {
	RequestID string    `json:"request_id"`
	TraceID   string    `json:"trace_id"`
	Timestamp time.Time `json:"timestamp"`
}

type observedWriter struct {
	http.ResponseWriter
	status int
	bytes  int
	meta   RequestMeta
}

func (w *observedWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *observedWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}
func (w *observedWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

type Principal struct {
	Actor       string
	TenantID    uuid.UUID
	Role        string
	Permissions map[string]bool
}
type EventHub struct {
	mu      sync.RWMutex
	clients map[chan []byte]struct{}
}

func NewEventHub() *EventHub { return &EventHub{clients: map[chan []byte]struct{}{}} }
func (h *EventHub) Publish(event string, data any) {
	b, _ := json.Marshal(data)
	msg := append([]byte("event: "+event+"\ndata: "), b...)
	msg = append(msg, []byte("\n\n")...)
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		select {
		case c <- msg:
		default:
		}
	}
}
func (h *EventHub) Stream(w http.ResponseWriter, r *http.Request) {
	f, ok := w.(http.Flusher)
	if !ok {
		problem(w, r, 500, "STREAMING_UNSUPPORTED", "Streaming unsupported", false)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	c := make(chan []byte, 32)
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	defer func() { h.mu.Lock(); delete(h.clients, c); h.mu.Unlock() }()
	w.Write([]byte(": connected\n\n"))
	f.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case data := <-c:
			w.Write(data)
			f.Flush()
		}
	}
}

type API struct {
	Store    *postgres.Store
	Business *tasks.Service
	Identity identity.Provider
	Events   *EventHub
}

func (a *API) business() *tasks.Service {
	if a.Business == nil {
		a.Business = tasks.New(a.Store)
	}
	return a.Business
}

func (a *API) Router() http.Handler {
	telemetry.RegisterMetrics()
	r := chi.NewRouter()
	r.Use(a.observability)
	r.Use(a.auth)
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	r.Handle("/metrics", promhttp.Handler())
	r.Get("/", dashboard)
	r.Group(func(r chi.Router) {
		r.Use(a.requireAuth, a.scope, a.authorize)
		r.Get("/api/v1/events", a.streamEvents)
		r.Post("/api/v1/retry-policies", a.createRetryPolicy)
		r.Post("/api/v1/rate-limit-policies", a.createRateLimitPolicy)
		r.Post("/api/v1/workflows", a.createWorkflow)
		r.Get("/api/v1/workflows", a.listWorkflows)
		r.Post("/api/v1/workflows/{id}/runs", a.startWorkflow)
		r.Delete("/api/v1/workflows/{id}", a.deleteWorkflow)
		r.Post("/api/v1/workflows/{id}/restore", a.restoreWorkflow)
		r.Post("/api/v1/function-definitions", a.createFunctionDefinition)
		r.Post("/api/v1/job-definitions", a.createJobDefinition)
		r.Post("/api/v1/job-definitions/{id}/run", a.runNow)
		r.Post("/api/v1/job-runs:bulk", a.runBulk)
		r.Get("/api/v1/job-runs", a.listRuns)
		r.Get("/api/v1/job-runs/{id}", a.getRun)
		r.Post("/api/v1/job-runs/{id}/cancel", a.cancelRun)
		r.Post("/api/v1/job-runs/{id}/retry", a.retryRun)
		r.Get("/api/v1/job-runs/{id}/attempts", a.listAttempts)
		r.Get("/api/v1/job-runs/{id}/logs", a.listJobLogs)
		r.Get("/api/v1/job-batches", a.listBatches)
		r.Get("/api/v1/job-batches/{id}", a.getBatch)
		r.Get("/api/v1/job-batches/{id}/items", a.batchItems)
		r.Get("/api/v1/job-batches/{id}/attempts", a.batchAttempts)
		r.Get("/api/v1/job-batches/{id}/logs", a.batchLogs)
		r.Post("/api/v1/queues", a.createQueue)
		r.Post("/api/v1/queues/{id}/pause", a.queueState("PAUSED", "queue.paused"))
		r.Post("/api/v1/queues/{id}/resume", a.queueState("ACTIVE", "queue.resumed"))
		r.Post("/api/v1/queues/{id}/drain", a.queueState("DRAINING", "queue.draining"))
		r.Post("/api/v1/schedules", a.createSchedule)
		r.Post("/api/v1/schedules/{id}/pause", a.scheduleState("PAUSED"))
		r.Post("/api/v1/schedules/{id}/resume", a.scheduleState("ACTIVE"))
		r.Get("/api/v1/dlq", a.listDLQ)
		r.Post("/api/v1/dlq/{id}/replay", a.replayDLQ)
		r.Get("/api/v1/workers", a.workers)
		r.Get("/api/v1/scheduler-logs", a.schedulerLogs)
		r.Get("/api/v1/audit-logs", a.audits)
		r.Get("/api/v1/rbac/permissions", a.rbacPermissions)
		r.Get("/api/v1/rbac/users", a.rbacUsers)
		r.Post("/api/v1/rbac/users", a.createRBACUser)
		r.Put("/api/v1/rbac/users/{id}/roles", a.replaceUserRoles)
		r.Get("/api/v1/rbac/roles", a.rbacRoles)
		r.Post("/api/v1/rbac/roles", a.createRBACRole)
		r.Put("/api/v1/rbac/roles/{id}/permissions", a.replaceRolePermissions)
		r.Get("/api/v1/{aggregate}", a.controlList)
		r.Get("/api/v1/{aggregate}/{id}", a.controlGet)
		r.Patch("/api/v1/{aggregate}/{id}", a.controlUpdate)
		r.Delete("/api/v1/{aggregate}/{id}", a.controlDelete)
		r.Post("/api/v1/{aggregate}/{id}/restore", a.controlRestore)
	})
	return r
}

// observability is deliberately the outermost middleware: every HTTP request,
// including auth failures, receives a stable request ID, W3C trace context and
// one structured completion log line.
func (a *API) observability(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if requestID == "" || len(requestID) > 128 {
			requestID = uuid.NewString()
		}
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := otel.Tracer("task-processing/api").Start(ctx, "http.request")
		defer span.End()
		traceID := span.SpanContext().TraceID().String()
		if traceID == "00000000000000000000000000000000" || traceID == "" {
			traceID = strings.ReplaceAll(uuid.NewString(), "-", "")
		}
		meta := RequestMeta{RequestID: requestID, TraceID: traceID, Timestamp: time.Now().UTC()}
		ctx = context.WithValue(ctx, requestMetaKey{}, meta)
		ww := &observedWriter{ResponseWriter: w, meta: meta}
		ww.Header().Set("X-Request-ID", requestID)
		ww.Header().Set("X-Trace-ID", traceID)
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(ww.Header()))
		started := time.Now()
		next.ServeHTTP(ww, r.WithContext(ctx))
		status := ww.status
		if status == 0 {
			status = http.StatusOK
		}
		span.SetAttributes(
			attribute.String("http.request.method", r.Method),
			attribute.String("url.path", r.URL.Path),
			attribute.Int("http.response.status_code", status),
			attribute.String("request.id", requestID),
		)
		if status >= 500 {
			span.SetStatus(codes.Error, http.StatusText(status))
		}
		slog.Info("http request completed", "request_id", requestID, "trace_id", traceID, "method", r.Method, "path", r.URL.Path, "status", status, "bytes", ww.bytes, "duration_ms", time.Since(started).Milliseconds())
	})
}
func (a *API) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.Identity != nil {
			subject, err := a.Identity.Authenticate(r)
			if err == nil {
				next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, Principal{Actor: subject.Actor, TenantID: subject.TenantID, Role: subject.Role})))
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (a *API) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := r.Context().Value(principalKey{}).(Principal); !ok {
			problem(w, r, 401, "UNAUTHENTICATED", "Authentication required", false)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (a *API) scope(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := principal(r)
		if a.Store != nil && a.Identity != nil {
			permissions, err := a.Store.EffectivePermissions(r.Context(), p.TenantID, p.Actor)
			if err != nil {
				handleErr(w, r, err)
				return
			}
			p.Permissions = permissions
		}
		ctx := context.WithValue(r.Context(), principalKey{}, p)
		next.ServeHTTP(w, r.WithContext(context.WithValue(ctx, apiKey{}, a)))
	})
}
func principal(r *http.Request) Principal { return r.Context().Value(principalKey{}).(Principal) }
func requireRole(w http.ResponseWriter, r *http.Request, roles ...string) bool {
	p := principal(r)
	if p.Permissions != nil {
		return true // route policy below is the authority for dynamic RBAC.
	}
	for _, role := range roles {
		if p.Role == role {
			return true
		}
	}
	problem(w, r, 403, "FORBIDDEN", "Insufficient role", false)
	return false
}
func (a *API) authorize(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := principal(r)
		if p.Permissions == nil {
			next.ServeHTTP(w, r)
			return
		} // explicit development fallback only
		required := routePermission(r)
		if required == "" || p.Permissions["platform:admin"] || p.Permissions[required] {
			next.ServeHTTP(w, r)
			return
		}
		problem(w, r, http.StatusForbidden, "FORBIDDEN", "Permission "+required+" is required", false)
	})
}
func routePermission(r *http.Request) string {
	path := r.URL.Path
	if strings.Contains(path, "/events") {
		return "event:read"
	}
	if strings.Contains(path, "/rbac/") {
		return "platform:admin"
	}
	if strings.Contains(path, "/workflows") {
		if r.Method == http.MethodGet {
			return "control:read"
		}
		return "control:write"
	}
	if strings.Contains(path, "/audit-logs") {
		return "audit:read"
	}
	if strings.Contains(path, "/workers") || strings.Contains(path, "/scheduler-logs") {
		return "operations:read"
	}
	if strings.Contains(path, "/dlq") {
		if r.Method == http.MethodGet {
			return "operations:read"
		}
		return "operations:write"
	}
	if strings.Contains(path, "/job-batches") {
		return "batch:read"
	}
	if strings.Contains(path, "/job-runs") {
		if r.Method == http.MethodGet {
			return "job:read"
		}
		if strings.Contains(path, "/cancel") || strings.Contains(path, "/retry") {
			return "job:operate"
		}
		return "job:submit"
	}
	if strings.Contains(path, "/job-definitions/") && strings.Contains(path, "/run") {
		return "job:submit"
	}
	if strings.Contains(path, "/queues") || strings.Contains(path, "/retry-policies") || strings.Contains(path, "/rate-limit-policies") || strings.Contains(path, "/function-definitions") || strings.Contains(path, "/job-definitions") || strings.Contains(path, "/schedules") {
		if r.Method == http.MethodGet {
			return "control:read"
		}
		return "control:write"
	}
	return ""
}
func (a *API) projectOK(ctx context.Context, p Principal, project uuid.UUID) bool {
	var one int
	return a.Store.Pool.QueryRow(ctx, "SELECT 1 FROM projects WHERE id=$1 AND tenant_id=$2 AND status='ACTIVE' AND deleted_at IS NULL", project, p.TenantID).Scan(&one) == nil
}
func (a *API) audit(ctx context.Context, p Principal, project uuid.UUID, action, typ string, id uuid.UUID, after any) {
	a.auditChange(ctx, p, project, action, typ, id, nil, after)
}
func (a *API) auditChange(ctx context.Context, p Principal, project uuid.UUID, action, typ string, id uuid.UUID, before, after any) {
	beforeJSON, _ := json.Marshal(before)
	b, _ := json.Marshal(after)
	meta := requestMeta(ctx)
	_, _ = a.Store.Pool.Exec(ctx, "INSERT INTO audit_logs(project_id,tenant_id,actor_id,action,resource_type,resource_id,before_data,after_data,request_id,trace_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)", project, p.TenantID, p.Actor, action, typ, id, beforeJSON, b, meta.RequestID, meta.TraceID)
}
func (a *API) stateSnapshot(ctx context.Context, project, id uuid.UUID, typ string) any {
	var status string
	var version int64
	var err error
	switch typ {
	case "job_run":
		err = a.Store.Pool.QueryRow(ctx, "SELECT status,row_version FROM job_runs WHERE id=$1 AND project_id=$2", id, project).Scan(&status, &version)
	case "queue":
		err = a.Store.Pool.QueryRow(ctx, "SELECT status,row_version FROM queues WHERE id=$1 AND project_id=$2", id, project).Scan(&status, &version)
	case "schedule":
		err = a.Store.Pool.QueryRow(ctx, "SELECT s.status,s.row_version FROM schedules s JOIN job_definitions jd ON jd.id=s.job_definition_id WHERE s.id=$1 AND jd.project_id=$2", id, project).Scan(&status, &version)
	}
	if err != nil {
		return nil
	}
	return map[string]any{"status": status, "version": version}
}
func (a *API) emit(ctx context.Context, project uuid.UUID, eventType, aggregateType string, id uuid.UUID, payload any) {
	if err := a.Store.WriteRealtimeEvent(ctx, project, eventType, aggregateType, id, payload); err != nil {
		return
	}
	if a.Events != nil {
		a.Events.Publish(eventType, payload)
	}
}
func (a *API) streamEvents(w http.ResponseWriter, r *http.Request) {
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	after := int64(0)
	if raw := r.URL.Query().Get("after_id"); raw != "" {
		if _, err := fmt.Sscan(raw, &after); err != nil || after < 0 {
			problem(w, r, 400, "INVALID_CURSOR", "after_id must be a positive integer", false)
			return
		}
	}
	f, ok := w.(http.Flusher)
	if !ok {
		problem(w, r, 500, "STREAMING_UNSUPPORTED", "Streaming unsupported", false)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		events, err := a.Store.ListRealtimeEvents(r.Context(), project, after, 100)
		if err != nil {
			return
		}
		for _, event := range events {
			data, _ := json.Marshal(event)
			_, _ = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.ID, event.EventType, data)
			after = event.ID
		}
		f.Flush()
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

type runRequest struct {
	ProjectID      uuid.UUID       `json:"project_id"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key"`
	Priority       *job.Priority   `json:"priority"`
}

func (a *API) createRetryPolicy(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer") {
		return
	}
	var req struct {
		ProjectID            uuid.UUID `json:"project_id"`
		Name                 string    `json:"name"`
		MaxAttempts          int       `json:"max_attempts"`
		Strategy             string    `json:"strategy"`
		InitialDelayMS       int64     `json:"initial_delay_ms"`
		Multiplier           float64   `json:"multiplier"`
		MaxDelayMS           int64     `json:"max_delay_ms"`
		JitterPct            float64   `json:"jitter_pct"`
		RetryTimeout         bool      `json:"retry_timeout"`
		RetryRateLimited     bool      `json:"retry_rate_limited"`
		RetryDependencyError bool      `json:"retry_dependency_error"`
		RetryValidationError bool      `json:"retry_validation_error"`
	}
	if !decode(w, r, &req) {
		return
	}
	if !a.projectOK(r.Context(), principal(r), req.ProjectID) {
		problem(w, r, 404, "PROJECT_NOT_FOUND", "Project not found", false)
		return
	}
	patch := controlplane.Patch{
		"name":                   json.RawMessage(fmt.Sprintf("%q", req.Name)),
		"max_attempts":           json.RawMessage(fmt.Sprint(req.MaxAttempts)),
		"strategy":               json.RawMessage(fmt.Sprintf("%q", req.Strategy)),
		"initial_delay_ms":       json.RawMessage(fmt.Sprint(req.InitialDelayMS)),
		"multiplier":             json.RawMessage(fmt.Sprint(req.Multiplier)),
		"max_delay_ms":           json.RawMessage(fmt.Sprint(req.MaxDelayMS)),
		"jitter_pct":             json.RawMessage(fmt.Sprint(req.JitterPct)),
		"retry_timeout":          json.RawMessage(fmt.Sprint(req.RetryTimeout)),
		"retry_rate_limited":     json.RawMessage(fmt.Sprint(req.RetryRateLimited)),
		"retry_dependency_error": json.RawMessage(fmt.Sprint(req.RetryDependencyError)),
		"retry_validation_error": json.RawMessage(fmt.Sprint(req.RetryValidationError)),
	}
	if err := controlplane.ValidatePatch("retry_policy", patch); err != nil {
		problem(w, r, 400, "INVALID_RETRY_POLICY", err.Error(), false)
		return
	}
	if err := controlplane.ValidateRetryPolicyValues(req.Strategy, req.InitialDelayMS, req.MaxDelayMS, req.Multiplier); err != nil {
		problem(w, r, 400, "INVALID_RETRY_POLICY", err.Error(), false)
		return
	}
	var id uuid.UUID
	var version int64
	err := a.Store.Pool.QueryRow(r.Context(), "INSERT INTO retry_policies(project_id,name,max_attempts,strategy,initial_delay_ms,multiplier,max_delay_ms,jitter_pct,retry_timeout,retry_rate_limited,retry_dependency_error,retry_validation_error) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING id,row_version", req.ProjectID, req.Name, req.MaxAttempts, req.Strategy, req.InitialDelayMS, req.Multiplier, req.MaxDelayMS, req.JitterPct, req.RetryTimeout, req.RetryRateLimited, req.RetryDependencyError, req.RetryValidationError).Scan(&id, &version)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	a.audit(r.Context(), principal(r), req.ProjectID, "retry_policy.create", "retry_policy", id, req)
	a.emit(r.Context(), req.ProjectID, "retry_policy.changed", "retry_policy", id, map[string]any{"id": id, "version": version})
	w.Header().Set("ETag", strconv.Quote(strconv.FormatInt(version, 10)))
	writeJSON(w, 201, map[string]any{"id": id, "status": "ACTIVE", "version": version})
}

func (a *API) createRateLimitPolicy(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer") {
		return
	}
	var req struct {
		ProjectID      uuid.UUID  `json:"project_id"`
		Name           string     `json:"name"`
		Scope          string     `json:"scope"`
		TargetID       *uuid.UUID `json:"target_id"`
		Capacity       int        `json:"capacity"`
		RefillTokens   int        `json:"refill_tokens"`
		RefillPeriodMS int64      `json:"refill_period_ms"`
	}
	if !decode(w, r, &req) {
		return
	}
	if !a.projectOK(r.Context(), principal(r), req.ProjectID) {
		problem(w, r, 404, "PROJECT_NOT_FOUND", "Project not found", false)
		return
	}
	if strings.TrimSpace(req.Name) == "" || req.Capacity < 1 || req.RefillTokens < 1 || req.RefillPeriodMS < 1 || req.RefillPeriodMS > 86400000 {
		problem(w, r, 400, "INVALID_RATE_LIMIT", "name, capacity, refill_tokens and refill_period_ms are invalid", false)
		return
	}
	if req.Scope != "PROJECT" && req.Scope != "QUEUE" && req.Scope != "FUNCTION" {
		problem(w, r, 400, "INVALID_RATE_LIMIT_SCOPE", "scope must be PROJECT, QUEUE or FUNCTION", false)
		return
	}
	if (req.Scope == "PROJECT") != (req.TargetID == nil) {
		problem(w, r, 400, "INVALID_RATE_LIMIT_TARGET", "PROJECT has no target; QUEUE/FUNCTION require target_id", false)
		return
	}
	if req.TargetID != nil {
		var exists bool
		table := "queues"
		if req.Scope == "FUNCTION" {
			table = "function_definitions"
		}
		err := a.Store.Pool.QueryRow(r.Context(), "SELECT EXISTS(SELECT 1 FROM "+table+" WHERE id=$1 AND project_id=$2 AND deleted_at IS NULL)", *req.TargetID, req.ProjectID).Scan(&exists)
		if err != nil || !exists {
			problem(w, r, 404, "RATE_LIMIT_TARGET_NOT_FOUND", "Rate-limit target is not active in this project", false)
			return
		}
	}
	tx, err := a.Store.Pool.Begin(r.Context())
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer tx.Rollback(r.Context())
	var id uuid.UUID
	var version int64
	err = tx.QueryRow(r.Context(), "INSERT INTO rate_limit_policies(project_id,name,scope,target_id,capacity,refill_tokens,refill_period_ms) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id,row_version", req.ProjectID, req.Name, req.Scope, req.TargetID, req.Capacity, req.RefillTokens, req.RefillPeriodMS).Scan(&id, &version)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	if _, err = tx.Exec(r.Context(), "INSERT INTO rate_limit_buckets(policy_id,tokens) VALUES($1,$2)", id, req.Capacity); err != nil {
		handleErr(w, r, err)
		return
	}
	if err = a.auditChangeTx(r.Context(), tx, principal(r), req.ProjectID, "rate_limit_policy.create", "rate_limit_policy", id, nil, req); err != nil {
		handleErr(w, r, err)
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		handleErr(w, r, err)
		return
	}
	a.emit(r.Context(), req.ProjectID, "rate_limit_policy.changed", "rate_limit_policy", id, map[string]any{"id": id, "version": version})
	w.Header().Set("ETag", strconv.Quote(strconv.FormatInt(version, 10)))
	writeJSON(w, 201, map[string]any{"id": id, "version": version, "status": "ACTIVE"})
}

func (a *API) createFunctionDefinition(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer") {
		return
	}
	var req struct {
		ProjectID   uuid.UUID       `json:"project_id"`
		FunctionKey string          `json:"function_key"`
		Version     string          `json:"version"`
		InputSchema json.RawMessage `json:"input_schema"`
	}
	if !decode(w, r, &req) {
		return
	}
	if !a.projectOK(r.Context(), principal(r), req.ProjectID) {
		problem(w, r, 404, "PROJECT_NOT_FOUND", "Project not found", false)
		return
	}
	if req.FunctionKey == "" || req.Version == "" {
		problem(w, r, 400, "INVALID_FUNCTION", "function_key and version are required", false)
		return
	}
	if len(req.InputSchema) == 0 {
		req.InputSchema = json.RawMessage(`{}`)
	}
	var id uuid.UUID
	err := a.Store.Pool.QueryRow(r.Context(), "INSERT INTO function_definitions(project_id,function_key,version,input_schema) VALUES($1,$2,$3,$4) RETURNING id", req.ProjectID, req.FunctionKey, req.Version, req.InputSchema).Scan(&id)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	a.audit(r.Context(), principal(r), req.ProjectID, "function.create", "function_definition", id, req)
	writeJSON(w, 201, map[string]any{"id": id, "status": "ACTIVE"})
}

func (a *API) createJobDefinition(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer") {
		return
	}
	var req struct {
		ProjectID       uuid.UUID  `json:"project_id"`
		FunctionID      uuid.UUID  `json:"function_id"`
		QueueID         uuid.UUID  `json:"queue_id"`
		RetryPolicyID   *uuid.UUID `json:"retry_policy_id"`
		Name            string     `json:"name"`
		DefaultPriority int        `json:"default_priority"`
		TimeoutMS       int64      `json:"timeout_ms"`
		ExecutionMode   string     `json:"execution_mode"`
		BatchSize       int        `json:"batch_size"`
		BatchMaxWaitMS  int        `json:"batch_max_wait_ms"`
	}
	if !decode(w, r, &req) {
		return
	}
	if !a.projectOK(r.Context(), principal(r), req.ProjectID) {
		problem(w, r, 404, "PROJECT_NOT_FOUND", "Project not found", false)
		return
	}
	if req.Name == "" {
		problem(w, r, 400, "INVALID_JOB_DEFINITION", "name is required", false)
		return
	}
	if req.DefaultPriority == 0 {
		req.DefaultPriority = 3
	}
	if req.TimeoutMS == 0 {
		req.TimeoutMS = 30000
	}
	if req.ExecutionMode == "" {
		req.ExecutionMode = "SINGLE"
	}
	if req.ExecutionMode != "SINGLE" && req.ExecutionMode != "BATCH" {
		problem(w, r, 400, "INVALID_EXECUTION_MODE", "execution_mode must be SINGLE or BATCH", false)
		return
	}
	if req.BatchSize == 0 {
		req.BatchSize = 100
	}
	if req.BatchSize < 2 || req.BatchSize > 1000 {
		problem(w, r, 400, "INVALID_BATCH_SIZE", "batch_size must be between 2 and 1000", false)
		return
	}
	if req.BatchMaxWaitMS < 0 || req.BatchMaxWaitMS > 3600000 {
		problem(w, r, 400, "INVALID_BATCH_WAIT", "batch_max_wait_ms must be 0..3600000", false)
		return
	}
	var id uuid.UUID
	err := a.Store.Pool.QueryRow(r.Context(), `INSERT INTO job_definitions(project_id,function_id,queue_id,retry_policy_id,name,default_priority,timeout_ms,execution_mode,batch_size,batch_max_wait_ms) SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10 WHERE EXISTS(SELECT 1 FROM function_definitions WHERE id=$2 AND project_id=$1 AND deleted_at IS NULL FOR UPDATE) AND EXISTS(SELECT 1 FROM queues WHERE id=$3 AND project_id=$1 AND deleted_at IS NULL FOR UPDATE) AND ($4 IS NULL OR EXISTS(SELECT 1 FROM retry_policies WHERE id=$4 AND project_id=$1 AND deleted_at IS NULL FOR UPDATE)) RETURNING id`, req.ProjectID, req.FunctionID, req.QueueID, req.RetryPolicyID, req.Name, req.DefaultPriority, req.TimeoutMS, req.ExecutionMode, req.BatchSize, req.BatchMaxWaitMS).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, r, 404, "DEPENDENCY_NOT_FOUND", "Function, queue or retry policy is outside this project", false)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	a.audit(r.Context(), principal(r), req.ProjectID, "job_definition.create", "job_definition", id, req)
	writeJSON(w, 201, map[string]any{"id": id, "status": "ACTIVE"})
}

func (a *API) runNow(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer", "operator") {
		return
	}
	var in runRequest
	if !decode(w, r, &in) {
		return
	}
	if !a.projectOK(r.Context(), principal(r), in.ProjectID) {
		problem(w, r, 404, "PROJECT_NOT_FOUND", "Project not found", false)
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_ID", "Invalid job definition ID", false)
		return
	}
	out, err := a.business().Jobs.Submit(r.Context(), tasks.SubmitInput{ProjectID: in.ProjectID, DefinitionID: id, Payload: in.Payload, IdempotencyKey: in.IdempotencyKey, Priority: in.Priority})
	if err != nil {
		handleErr(w, r, err)
		return
	}
	a.audit(r.Context(), principal(r), in.ProjectID, "job.run", "job_run", out.ID, out)
	a.emit(r.Context(), in.ProjectID, "job.created", "job_run", out.ID, out)
	writeJSON(w, 202, out)
}
func (a *API) runBulk(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer", "operator") {
		return
	}
	var req struct {
		ProjectID    uuid.UUID `json:"project_id"`
		DefinitionID uuid.UUID `json:"job_definition_id"`
		Items        []struct {
			Payload        json.RawMessage `json:"payload"`
			IdempotencyKey string          `json:"idempotency_key"`
			Priority       *job.Priority   `json:"priority"`
		} `json:"items"`
	}
	if !decode(w, r, &req) {
		return
	}
	if !a.projectOK(r.Context(), principal(r), req.ProjectID) {
		problem(w, r, 404, "PROJECT_NOT_FOUND", "Project not found", false)
		return
	}
	items := make([]tasks.SubmitInput, 0, len(req.Items))
	for _, v := range req.Items {
		items = append(items, tasks.SubmitInput{Payload: v.Payload, IdempotencyKey: v.IdempotencyKey, Priority: v.Priority})
	}
	runs, err := a.business().Jobs.SubmitBulk(r.Context(), req.ProjectID, req.DefinitionID, items)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	for _, v := range runs {
		a.audit(r.Context(), principal(r), req.ProjectID, "job.bulk_run", "job_run", v.ID, v)
	}
	for _, run := range runs {
		a.emit(r.Context(), req.ProjectID, "job.created", "job_run", run.ID, run)
	}
	writeJSON(w, 202, map[string]any{"batch_id": uuid.New(), "runs": runs})
}
func (a *API) listRuns(w http.ResponseWriter, r *http.Request) {
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	rows, err := a.Store.Pool.Query(r.Context(), `SELECT id,status,priority,row_version,available_at,created_at,started_at,finished_at FROM job_runs WHERE project_id=$1 ORDER BY created_at DESC LIMIT 200`, project)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id uuid.UUID
		var status string
		var priority int
		var version int64
		var available, created time.Time
		var started, finished *time.Time
		if err = rows.Scan(&id, &status, &priority, &version, &available, &created, &started, &finished); err != nil {
			handleErr(w, r, err)
			return
		}
		out = append(out, map[string]any{"id": id, "status": status, "priority": priority, "version": version, "available_at": available, "created_at": created, "started_at": started, "finished_at": finished})
	}
	writeJSON(w, 200, out)
}
func (a *API) getRun(w http.ResponseWriter, r *http.Request) {
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_ID", "Invalid run ID", false)
		return
	}
	var status string
	var payload json.RawMessage
	var priority int
	var version int64
	var created time.Time
	err = a.Store.Pool.QueryRow(r.Context(), "SELECT status,payload,priority,row_version,created_at FROM job_runs WHERE id=$1 AND project_id=$2", id, project).Scan(&status, &payload, &priority, &version, &created)
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, r, 404, "RUN_NOT_FOUND", "Run not found", false)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	w.Header().Set("ETag", strconv.Quote(strconv.FormatInt(version, 10)))
	writeJSON(w, 200, map[string]any{"id": id, "status": status, "payload": json.RawMessage(payload), "priority": priority, "version": version, "created_at": created})
}
func (a *API) cancelRun(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer", "operator") {
		return
	}
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_ID", "Invalid run ID", false)
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	before := a.stateSnapshot(r.Context(), project, id, "job_run")
	if err := a.business().Jobs.Cancel(r.Context(), project, id, version); errors.Is(err, tasks.ErrInvalidState) {
		problem(w, r, 412, "PRECONDITION_FAILED", "Run version or lifecycle state no longer matches", false)
		return
	} else if err != nil {
		handleErr(w, r, err)
		return
	}
	a.auditChange(r.Context(), principal(r), project, "job.cancel", "job_run", id, before, map[string]any{"status": "CANCELLED", "version": version + 1})
	a.emit(r.Context(), project, "job.cancelled", "job_run", id, map[string]any{"id": id})
	w.Header().Set("ETag", strconv.Quote(strconv.FormatInt(version+1, 10)))
	writeJSON(w, 200, map[string]any{"status": "CANCELLED", "version": version + 1})
}
func (a *API) retryRun(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "operator") {
		return
	}
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_ID", "Invalid run ID", false)
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	before := a.stateSnapshot(r.Context(), project, id, "job_run")
	if err := a.business().Jobs.Retry(r.Context(), project, id, version); errors.Is(err, tasks.ErrInvalidState) {
		problem(w, r, 412, "PRECONDITION_FAILED", "Run version or lifecycle state no longer matches", false)
		return
	} else if err != nil {
		handleErr(w, r, err)
		return
	}
	a.auditChange(r.Context(), principal(r), project, "job.retry", "job_run", id, before, map[string]any{"status": "QUEUED", "version": version + 1})
	a.emit(r.Context(), project, "job.queued", "job_run", id, map[string]any{"id": id})
	w.Header().Set("ETag", strconv.Quote(strconv.FormatInt(version+1, 10)))
	writeJSON(w, 202, map[string]any{"status": "QUEUED", "version": version + 1})
}
func (a *API) listAttempts(w http.ResponseWriter, r *http.Request) {
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_ID", "Invalid run ID", false)
		return
	}
	rows, err := a.Store.Pool.Query(r.Context(), `SELECT a.id,a.attempt_number,a.status,a.error_class,a.error_code,a.error_message,a.progress_pct,a.progress_message,a.started_at,a.finished_at FROM job_attempts a JOIN job_runs r ON r.id=a.job_run_id WHERE a.job_run_id=$1 AND r.project_id=$2 ORDER BY a.attempt_number`, id, project)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var aid uuid.UUID
		var number int
		var status string
		var cl, code, msg *string
		var progress int
		var progressMessage *string
		var started time.Time
		var finished *time.Time
		if err = rows.Scan(&aid, &number, &status, &cl, &code, &msg, &progress, &progressMessage, &started, &finished); err != nil {
			handleErr(w, r, err)
			return
		}
		out = append(out, map[string]any{"id": aid, "number": number, "status": status, "error_class": cl, "error_code": code, "error_message": msg, "progress_pct": progress, "progress_message": progressMessage, "started_at": started, "finished_at": finished})
	}
	writeJSON(w, 200, out)
}
func (a *API) listJobLogs(w http.ResponseWriter, r *http.Request) {
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_ID", "Invalid run ID", false)
		return
	}
	level := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("level")))
	if level != "" && level != "DEBUG" && level != "INFO" && level != "WARN" && level != "ERROR" {
		problem(w, r, 400, "INVALID_LOG_LEVEL", "level must be DEBUG, INFO, WARN or ERROR", false)
		return
	}
	rows, err := a.Store.Pool.Query(r.Context(), "SELECT l.id,l.attempt_id,l.level,l.message,l.fields,l.trace_id,l.created_at FROM job_logs l WHERE l.project_id=$1 AND l.job_run_id=$2 AND (NULLIF($3,'') IS NULL OR l.level=$3) ORDER BY l.created_at DESC,l.id DESC LIMIT 500", project, id, level)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	out := make([]map[string]any, 0)
	for rows.Next() {
		var logID int64
		var attempt *uuid.UUID
		var itemLevel, msg string
		var fields json.RawMessage
		var traceID *string
		var created time.Time
		if err = rows.Scan(&logID, &attempt, &itemLevel, &msg, &fields, &traceID, &created); err != nil {
			handleErr(w, r, err)
			return
		}
		out = append(out, map[string]any{"id": logID, "attempt_id": attempt, "level": itemLevel, "message": msg, "fields": fields, "trace_id": traceID, "created_at": created})
	}
	if err = rows.Err(); err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
func (a *API) listBatches(w http.ResponseWriter, r *http.Request) {
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	rows, err := a.Store.Pool.Query(r.Context(), `SELECT id,status,total_items,processed_items,succeeded_items,failed_items,retry_scheduled_items,created_at,started_at,finished_at FROM job_batches WHERE project_id=$1 ORDER BY created_at DESC LIMIT 200`, project)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id uuid.UUID
		var status string
		var total, processed, succeeded, failed, retried int
		var created time.Time
		var started, finished *time.Time
		if err = rows.Scan(&id, &status, &total, &processed, &succeeded, &failed, &retried, &created, &started, &finished); err != nil {
			handleErr(w, r, err)
			return
		}
		out = append(out, map[string]any{"id": id, "status": status, "total_items": total, "processed_items": processed, "succeeded_items": succeeded, "failed_items": failed, "retry_scheduled_items": retried, "created_at": created, "started_at": started, "finished_at": finished})
	}
	writeJSON(w, 200, out)
}
func (a *API) getBatch(w http.ResponseWriter, r *http.Request) {
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_BATCH_ID", "Invalid batch ID", false)
		return
	}
	var status string
	var total, processed, succeeded, failed, retried int
	var created time.Time
	var started, finished *time.Time
	err = a.Store.Pool.QueryRow(r.Context(), `SELECT status,total_items,processed_items,succeeded_items,failed_items,retry_scheduled_items,created_at,started_at,finished_at FROM job_batches WHERE id=$1 AND project_id=$2`, id, project).Scan(&status, &total, &processed, &succeeded, &failed, &retried, &created, &started, &finished)
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, r, 404, "BATCH_NOT_FOUND", "Batch not found", false)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"id": id, "status": status, "total_items": total, "processed_items": processed, "succeeded_items": succeeded, "failed_items": failed, "retry_scheduled_items": retried, "progress_pct": processed * 100 / total, "created_at": created, "started_at": started, "finished_at": finished})
}
func (a *API) batchItems(w http.ResponseWriter, r *http.Request) {
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_BATCH_ID", "Invalid batch ID", false)
		return
	}
	rows, err := a.Store.Pool.Query(r.Context(), `SELECT bi.id,bi.job_run_id,bi.ordinal,bi.status,bi.error_class,bi.error_code,bi.error_message,bi.completed_at FROM job_batch_items bi JOIN job_batches b ON b.id=bi.batch_id WHERE bi.batch_id=$1 AND b.project_id=$2 ORDER BY bi.ordinal`, id, project)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var item, run uuid.UUID
		var ordinal int
		var status string
		var class, code, msg *string
		var done *time.Time
		if err = rows.Scan(&item, &run, &ordinal, &status, &class, &code, &msg, &done); err != nil {
			handleErr(w, r, err)
			return
		}
		out = append(out, map[string]any{"id": item, "job_run_id": run, "ordinal": ordinal, "status": status, "error_class": class, "error_code": code, "error_message": msg, "completed_at": done})
	}
	writeJSON(w, 200, out)
}
func (a *API) batchAttempts(w http.ResponseWriter, r *http.Request) {
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_BATCH_ID", "Invalid batch ID", false)
		return
	}
	rows, err := a.Store.Pool.Query(r.Context(), `SELECT a.id,a.attempt_number,a.status,a.total_items,a.processed_items,a.succeeded_items,a.failed_items,a.progress_pct,a.progress_message,a.started_at,a.finished_at FROM job_batch_attempts a JOIN job_batches b ON b.id=a.batch_id WHERE a.batch_id=$1 AND b.project_id=$2 ORDER BY a.attempt_number`, id, project)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var aid uuid.UUID
		var number, total, processed, success, failed, pct int
		var status string
		var message *string
		var started time.Time
		var finished *time.Time
		if err = rows.Scan(&aid, &number, &status, &total, &processed, &success, &failed, &pct, &message, &started, &finished); err != nil {
			handleErr(w, r, err)
			return
		}
		out = append(out, map[string]any{"id": aid, "attempt_number": number, "status": status, "total_items": total, "processed_items": processed, "succeeded_items": success, "failed_items": failed, "progress_pct": pct, "progress_message": message, "started_at": started, "finished_at": finished})
	}
	writeJSON(w, 200, out)
}
func (a *API) batchLogs(w http.ResponseWriter, r *http.Request) {
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_BATCH_ID", "Invalid batch ID", false)
		return
	}
	rows, err := a.Store.Pool.Query(r.Context(), `SELECT l.id,l.batch_attempt_id,l.level,l.message,l.fields,l.created_at FROM job_batch_logs l JOIN job_batches b ON b.id=l.batch_id WHERE l.batch_id=$1 AND b.project_id=$2 ORDER BY l.id DESC LIMIT 500`, id, project)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var logID int64
		var attempt *uuid.UUID
		var level, message string
		var fields json.RawMessage
		var at time.Time
		if err = rows.Scan(&logID, &attempt, &level, &message, &fields, &at); err != nil {
			handleErr(w, r, err)
			return
		}
		out = append(out, map[string]any{"id": logID, "batch_attempt_id": attempt, "level": level, "message": message, "fields": fields, "created_at": at})
	}
	writeJSON(w, 200, out)
}
func (a *API) createQueue(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin") {
		return
	}
	var req struct {
		ProjectID       uuid.UUID `json:"project_id"`
		Name            string    `json:"name"`
		MaxConcurrency  int       `json:"max_concurrency"`
		DefaultPriority int       `json:"default_priority"`
	}
	if !decode(w, r, &req) {
		return
	}
	if !a.projectOK(r.Context(), principal(r), req.ProjectID) {
		problem(w, r, 404, "PROJECT_NOT_FOUND", "Project not found", false)
		return
	}
	id, err := a.business().Queues.Create(r.Context(), tasks.QueueInput{ProjectID: req.ProjectID, Name: req.Name, MaxConcurrency: req.MaxConcurrency, DefaultPriority: req.DefaultPriority})
	if err != nil {
		handleErr(w, r, err)
		return
	}
	a.audit(r.Context(), principal(r), req.ProjectID, "queue.create", "queue", id, req)
	writeJSON(w, 201, map[string]any{"id": id, "status": "ACTIVE"})
}
func (a *API) queueState(state, event string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireRole(w, r, "admin", "operator") {
			return
		}
		project, ok := projectParam(w, r)
		if !ok {
			return
		}
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			problem(w, r, 400, "INVALID_ID", "Invalid queue ID", false)
			return
		}
		version, ok := ifMatch(w, r)
		if !ok {
			return
		}
		before := a.stateSnapshot(r.Context(), project, id, "queue")
		var transitionErr error
		switch state {
		case "PAUSED":
			transitionErr = a.business().Queues.Pause(r.Context(), project, id, version)
		case "ACTIVE":
			transitionErr = a.business().Queues.Resume(r.Context(), project, id, version)
		case "DRAINING":
			transitionErr = a.business().Queues.Drain(r.Context(), project, id, version)
		}
		if errors.Is(transitionErr, tasks.ErrInvalidState) {
			problem(w, r, 412, "PRECONDITION_FAILED", "Queue version or lifecycle state no longer matches", false)
			return
		}
		if transitionErr != nil {
			handleErr(w, r, transitionErr)
			return
		}
		a.auditChange(r.Context(), principal(r), project, "queue."+state, "queue", id, before, map[string]any{"status": state, "version": version + 1})
		a.emit(r.Context(), project, event, "queue", id, map[string]any{"id": id, "status": state})
		w.Header().Set("ETag", strconv.Quote(strconv.FormatInt(version+1, 10)))
		writeJSON(w, 200, map[string]any{"status": state, "version": version + 1})
	}
}
func (a *API) createSchedule(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer") {
		return
	}
	var req struct {
		ProjectID       uuid.UUID `json:"project_id"`
		JobDefinitionID uuid.UUID `json:"job_definition_id"`
		Cron            string    `json:"cron_expression"`
		Timezone        string    `json:"timezone"`
		WithSeconds     bool      `json:"with_seconds"`
	}
	if !decode(w, r, &req) {
		return
	}
	if !a.projectOK(r.Context(), principal(r), req.ProjectID) {
		problem(w, r, 404, "PROJECT_NOT_FOUND", "Project not found", false)
		return
	}
	id, err := a.business().Schedules.Create(r.Context(), tasks.ScheduleInput{ProjectID: req.ProjectID, DefinitionID: req.JobDefinitionID, Cron: req.Cron, Timezone: req.Timezone, WithSeconds: req.WithSeconds})
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, r, 404, "JOB_DEFINITION_NOT_FOUND", "Job definition not found", false)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	a.audit(r.Context(), principal(r), req.ProjectID, "schedule.create", "schedule", id, req)
	a.emit(r.Context(), req.ProjectID, "schedule.changed", "schedule", id, map[string]any{"id": id})
	writeJSON(w, 201, map[string]any{"id": id, "status": "ACTIVE"})
}
func (a *API) scheduleState(state string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireRole(w, r, "admin", "developer", "operator") {
			return
		}
		project, ok := projectParam(w, r)
		if !ok {
			return
		}
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			problem(w, r, 400, "INVALID_ID", "Invalid schedule ID", false)
			return
		}
		version, ok := ifMatch(w, r)
		if !ok {
			return
		}
		before := a.stateSnapshot(r.Context(), project, id, "schedule")
		var stateErr error
		if state == "PAUSED" {
			stateErr = a.business().Schedules.Pause(r.Context(), project, id, version)
		} else {
			stateErr = a.business().Schedules.Resume(r.Context(), project, id, version)
		}
		if errors.Is(stateErr, tasks.ErrNotFound) {
			problem(w, r, 412, "PRECONDITION_FAILED", "Schedule version no longer matches", false)
			return
		}
		if stateErr != nil {
			handleErr(w, r, stateErr)
			return
		}
		a.auditChange(r.Context(), principal(r), project, "schedule."+state, "schedule", id, before, map[string]any{"status": state, "version": version + 1})
		a.emit(r.Context(), project, "schedule.changed", "schedule", id, map[string]any{"id": id, "status": state})
		w.Header().Set("ETag", strconv.Quote(strconv.FormatInt(version+1, 10)))
		writeJSON(w, 200, map[string]any{"status": state, "version": version + 1})
	}
}
func (a *API) listDLQ(w http.ResponseWriter, r *http.Request) {
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	rows, err := a.Store.Pool.Query(r.Context(), `SELECT d.id,d.job_run_id,d.reason,d.row_version,d.entered_at FROM dlq_entries d JOIN job_runs r ON r.id=d.job_run_id WHERE r.project_id=$1 AND d.replayed_at IS NULL ORDER BY d.entered_at DESC`, project)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id, rid uuid.UUID
		var reason string
		var version int64
		var entered time.Time
		if err = rows.Scan(&id, &rid, &reason, &version, &entered); err != nil {
			handleErr(w, r, err)
			return
		}
		out = append(out, map[string]any{"id": id, "job_run_id": rid, "reason": reason, "version": version, "entered_at": entered})
	}
	writeJSON(w, 200, out)
}
func (a *API) replayDLQ(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "operator") {
		return
	}
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_ID", "Invalid DLQ ID", false)
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	run, err := a.business().DLQ.Replay(r.Context(), project, id, version)
	if errors.Is(err, tasks.ErrNotFound) {
		problem(w, r, 404, "DLQ_NOT_FOUND", "DLQ entry not found", false)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	a.auditChange(r.Context(), principal(r), project, "dlq.replay", "dlq_entry", id, map[string]any{"replayed_at": nil, "version": version}, map[string]any{"job_run_id": run, "replayed_at": "now", "version": version + 1})
	a.emit(r.Context(), project, "job.queued", "job_run", run, map[string]any{"id": run})
	w.Header().Set("ETag", strconv.Quote(strconv.FormatInt(version+1, 10)))
	writeJSON(w, 202, map[string]any{"job_run_id": run, "status": "QUEUED", "version": version + 1})
}
func (a *API) workers(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "operator", "viewer", "developer") {
		return
	}
	rows, err := a.Store.Pool.Query(r.Context(), "SELECT id,worker_key,hostname,version,status,heartbeat_at FROM workers ORDER BY heartbeat_at DESC LIMIT 200")
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id uuid.UUID
		var key, host, ver, status string
		var heartbeat time.Time
		if err = rows.Scan(&id, &key, &host, &ver, &status, &heartbeat); err != nil {
			handleErr(w, r, err)
			return
		}
		out = append(out, map[string]any{"id": id, "worker_key": key, "hostname": host, "version": ver, "status": status, "heartbeat_at": heartbeat})
	}
	writeJSON(w, 200, out)
}
func (a *API) schedulerLogs(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "operator", "developer", "viewer") {
		return
	}
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	filter := postgres.SchedulerLogFilter{ProjectID: project, Level: r.URL.Query().Get("level")}
	if raw := r.URL.Query().Get("schedule_id"); raw != "" {
		id, err := uuid.Parse(raw)
		if err != nil {
			problem(w, r, 400, "INVALID_SCHEDULE_ID", "schedule_id must be a UUID", false)
			return
		}
		filter.ScheduleID = &id
	}
	if raw := r.URL.Query().Get("before"); raw != "" {
		before, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			problem(w, r, 400, "INVALID_CURSOR", "before must use RFC3339", false)
			return
		}
		filter.Before = &before
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		var n int
		if _, err := fmt.Sscanf(raw, "%d", &n); err != nil || n < 1 || n > 500 {
			problem(w, r, 400, "INVALID_LIMIT", "limit must be between 1 and 500", false)
			return
		}
		filter.Limit = n
	}
	logs, err := a.Store.ListSchedulerLogs(r.Context(), filter)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": logs, "next_before": func() any {
		if len(logs) == 0 {
			return nil
		}
		return logs[len(logs)-1].OccurredAt
	}()})
}
func (a *API) audits(w http.ResponseWriter, r *http.Request) {
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	rows, err := a.Store.Pool.Query(r.Context(), "SELECT id,actor_id,action,resource_type,resource_id,created_at FROM audit_logs WHERE project_id=$1 ORDER BY created_at DESC LIMIT 200", project)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id, rid uuid.UUID
		var actor, action, typ string
		var at time.Time
		if err = rows.Scan(&id, &actor, &action, &typ, &rid, &at); err != nil {
			handleErr(w, r, err)
			return
		}
		out = append(out, map[string]any{"id": id, "actor_id": actor, "action": action, "resource_type": typ, "resource_id": rid, "created_at": at})
	}
	writeJSON(w, 200, out)
}
func projectParam(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	raw := r.URL.Query().Get("project_id")
	id, err := uuid.Parse(raw)
	if err != nil {
		problem(w, r, 400, "PROJECT_ID_REQUIRED", "A valid project_id query parameter is required", false)
		return uuid.Nil, false
	}
	api := r.Context().Value(apiKey{}).(*API)
	if !api.projectOK(r.Context(), principal(r), id) {
		problem(w, r, 404, "PROJECT_NOT_FOUND", "Project not found", false)
		return uuid.Nil, false
	}
	return id, true
}

// ifMatch enforces optimistic locking for externally initiated mutations.
// Clients send the version returned by a read endpoint as If-Match: "42".
func ifMatch(w http.ResponseWriter, r *http.Request) (int64, bool) {
	raw := strings.Trim(strings.TrimSpace(r.Header.Get("If-Match")), "\"")
	version, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || version < 1 {
		problem(w, r, http.StatusPreconditionRequired, "PRECONDITION_REQUIRED", "If-Match must contain the current positive entity version", false)
		return 0, false
	}
	return version, true
}

type apiKey struct{}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		problem(w, r, 400, "INVALID_JSON", "Request body must be valid JSON", false)
		return false
	}
	return true
}

type responseEnvelope struct {
	Data any         `json:"data"`
	Meta RequestMeta `json:"meta"`
}
type errorEnvelope struct {
	Error map[string]any `json:"error"`
	Meta  RequestMeta    `json:"meta"`
}

func requestMeta(ctx context.Context) RequestMeta {
	if meta, ok := ctx.Value(requestMetaKey{}).(RequestMeta); ok {
		return meta
	}
	return RequestMeta{RequestID: uuid.NewString(), TraceID: strings.ReplaceAll(uuid.NewString(), "-", ""), Timestamp: time.Now().UTC()}
}
func responseMeta(w http.ResponseWriter) RequestMeta {
	if observed, ok := w.(*observedWriter); ok {
		return observed.meta
	}
	return RequestMeta{RequestID: uuid.NewString(), TraceID: strings.ReplaceAll(uuid.NewString(), "-", ""), Timestamp: time.Now().UTC()}
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(responseEnvelope{Data: v, Meta: responseMeta(w)})
}
func problem(w http.ResponseWriter, r *http.Request, status int, code, title string, retryable bool) {
	w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorEnvelope{Error: map[string]any{"type": "urn:task-platform:error:" + code, "title": title, "status": status, "detail": title, "instance": r.URL.Path, "code": code, "retryable": retryable}, Meta: requestMeta(r.Context())})
}
func handleErr(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, r, 404, "NOT_FOUND", "Resource not found", false)
		return
	}
	problem(w, r, 409, "CONFLICT", fmt.Sprintf("Request could not be applied: %v", err), true)
}
func dashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(`<!doctype html><title>Task Processing</title><style>body{font-family:system-ui;max-width:960px;margin:3rem auto;background:#0b1020;color:#e5e7eb}code{background:#182038;padding:.2rem}.card{padding:1rem;border:1px solid #334155;border-radius:.5rem;margin:1rem 0}</style><h1>Task Processing</h1><div class=card>Durable PostgreSQL control plane is online. Use the REST API with scoped identity headers.</div><div class=card><code>GET /api/v1/job-runs?project_id=...</code><br><code>GET /api/v1/events?project_id=...&after_id=0</code> streams durable state changes.<br><code>GET /metrics</code> exposes Prometheus metrics.</div>`))
}
