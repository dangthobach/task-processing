package postgres

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/google/uuid"
)

func (s *Store) WriteJobLog(ctx context.Context, project, run, attempt uuid.UUID, level, message, traceID string, fields map[string]any) error {
	if len(message) > 2000 {
		message = message[:2000]
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	_, err = s.Pool.Exec(ctx, "INSERT INTO job_logs(project_id,job_run_id,attempt_id,level,message,fields,trace_id) VALUES($1,$2,$3,$4,$5,$6,NULLIF($7,''))", project, run, attempt, strings.ToUpper(level), message, raw, traceID)
	return err
}
