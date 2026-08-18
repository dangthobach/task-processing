package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/example/task-processing/internal/application/controlplane"
	"github.com/robfig/cron/v3"
)

// previewSchedule deliberately uses the same parser options as schedule
// creation.  The UI therefore previews the exact occurrences the planner
// will reconcile, rather than a browser-specific approximation.
func (a *API) previewSchedule(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer", "operator", "viewer") {
		return
	}
	var req struct {
		CronExpression string `json:"cron_expression"`
		Timezone       string `json:"timezone"`
		WithSeconds    bool   `json:"with_seconds"`
		From           string `json:"from"`
	}
	if !decode(w, r, &req) {
		return
	}
	req.CronExpression = strings.TrimSpace(req.CronExpression)
	if err := controlplane.ValidateScheduleExpression(req.CronExpression, req.Timezone, req.WithSeconds); err != nil {
		problem(w, r, http.StatusBadRequest, "INVALID_SCHEDULE", err.Error(), false)
		return
	}
	loc, _ := time.LoadLocation(req.Timezone)
	start := time.Now().UTC()
	if req.From != "" {
		parsed, err := time.Parse(time.RFC3339, req.From)
		if err != nil {
			problem(w, r, http.StatusBadRequest, "INVALID_PREVIEW_START", "from must use RFC3339", false)
			return
		}
		start = parsed.UTC()
	}
	options := cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow
	if req.WithSeconds {
		options |= cron.Second
	}
	schedule, err := cron.NewParser(options).Parse(req.CronExpression)
	if err != nil {
		problem(w, r, http.StatusBadRequest, "INVALID_SCHEDULE", "cron expression is invalid", false)
		return
	}
	cursor := start.In(loc)
	occurrences := make([]time.Time, 0, 5)
	for range 5 {
		cursor = schedule.Next(cursor)
		occurrences = append(occurrences, cursor.UTC())
	}
	writeJSON(w, http.StatusOK, map[string]any{"timezone": loc.String(), "occurrences": occurrences})
}
