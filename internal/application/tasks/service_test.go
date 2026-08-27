package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/example/task-processing/internal/domain/job"
	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/google/uuid"
)

type fakeRepo struct {
	queued          bool
	state           string
	submitted       []postgres.Submit
	createdSchedule postgres.CreateSchedule
}

func (f *fakeRepo) Submit(_ context.Context, in postgres.Submit) (job.Run, error) {
	f.submitted = []postgres.Submit{in}
	return job.Run{ID: uuid.New()}, nil
}
func (f *fakeRepo) SubmitBulk(_ context.Context, in []postgres.Submit) ([]job.Run, error) {
	f.submitted = in
	return []job.Run{{ID: uuid.New()}}, nil
}
func (f *fakeRepo) CancelRun(context.Context, uuid.UUID, uuid.UUID, int64) (bool, error) {
	return f.queued, nil
}
func (f *fakeRepo) RetryRun(context.Context, uuid.UUID, uuid.UUID, int64) (bool, error) {
	return f.queued, nil
}
func (f *fakeRepo) CreateQueue(context.Context, postgres.CreateQueue) (uuid.UUID, error) {
	return uuid.New(), nil
}
func (f *fakeRepo) TransitionQueue(_ context.Context, _, _ uuid.UUID, to string, _ []string, _ int64) (bool, error) {
	f.state = to
	return f.queued, nil
}
func (f *fakeRepo) CreateSchedule(_ context.Context, in postgres.CreateSchedule) (uuid.UUID, error) {
	f.createdSchedule = in
	return uuid.New(), nil
}
func (f *fakeRepo) TransitionSchedule(context.Context, uuid.UUID, uuid.UUID, string, int64) (bool, error) {
	return f.queued, nil
}
func (f *fakeRepo) ReplayDLQ(context.Context, uuid.UUID, uuid.UUID, int64) (uuid.UUID, bool, error) {
	return uuid.New(), f.queued, nil
}

func TestSubmitRejectsInvalidPriorityBeforeRepository(t *testing.T) {
	repo := &fakeRepo{}
	p := job.Priority(7)
	_, err := New(repo).Jobs.Submit(context.Background(), SubmitInput{ProjectID: uuid.New(), DefinitionID: uuid.New(), Payload: json.RawMessage(`{}`), Priority: &p})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("err=%v", err)
	}
	if len(repo.submitted) != 0 {
		t.Fatal("repository called")
	}
}
func TestBulkIsSingleRepositoryCall(t *testing.T) {
	repo := &fakeRepo{}
	project, definition := uuid.New(), uuid.New()
	_, err := New(repo).Jobs.SubmitBulk(context.Background(), project, definition, []SubmitInput{{Payload: json.RawMessage(`{"x":1}`)}, {Payload: json.RawMessage(`{"x":2}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(repo.submitted) != 2 || repo.submitted[0].ProjectID != project {
		t.Fatalf("commands=%+v", repo.submitted)
	}
}
func TestQueueLifecycleRejectsIllegalTransition(t *testing.T) {
	repo := &fakeRepo{queued: false}
	err := New(repo).Queues.Pause(context.Background(), uuid.New(), uuid.New(), 1)
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err=%v", err)
	}
}
func TestScheduleValidatesCronAndTimezone(t *testing.T) {
	repo := &fakeRepo{}
	_, err := New(repo).Schedules.Create(context.Background(), ScheduleInput{ProjectID: uuid.New(), DefinitionID: uuid.New(), Cron: "not cron", Timezone: "UTC"})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("cron err=%v", err)
	}
	_, err = New(repo).Schedules.Create(context.Background(), ScheduleInput{ProjectID: uuid.New(), DefinitionID: uuid.New(), Cron: "* * * * *", Timezone: "Mars/Olympus"})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("tz err=%v", err)
	}
}

func TestScheduleCreatesCanonicalOneTime(t *testing.T) {
	repo := &fakeRepo{}
	runAt := time.Now().Add(time.Minute).In(time.FixedZone("UTC+7", 7*60*60))
	_, err := New(repo).Schedules.Create(context.Background(), ScheduleInput{ProjectID: uuid.New(), DefinitionID: uuid.New(), Type: "ONE_TIME", RunAt: &runAt})
	if err != nil {
		t.Fatal(err)
	}
	if repo.createdSchedule.Type != "ONE_TIME" || repo.createdSchedule.RunAt == nil || repo.createdSchedule.RunAt.Location() != time.UTC || repo.createdSchedule.MisfirePolicy != "FIRE_ONCE" {
		t.Fatalf("schedule=%+v", repo.createdSchedule)
	}
}

func TestScheduleRejectsPastOneTime(t *testing.T) {
	past := time.Now().Add(-time.Second)
	_, err := New(&fakeRepo{}).Schedules.Create(context.Background(), ScheduleInput{ProjectID: uuid.New(), DefinitionID: uuid.New(), Type: "ONE_TIME", RunAt: &past})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("err=%v", err)
	}
}
