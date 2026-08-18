package postgres

import (
	"os"
	"strings"
	"testing"
)

func TestWorkflowDispatchReadsRuntimeGraphAndExecutionSnapshot(t *testing.T) {
	source, err := os.ReadFile("workflow.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, want := range []string{
		"workflow_run_edges",
		"nr.execution_snapshot",
		"Snapshot: &captured",
		`key := "workflow:" + run.String() + ":" + nodeRun.String()`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing workflow snapshot contract %q", want)
		}
	}
	if strings.Contains(text, "JOIN workflow_edges e JOIN workflow_node_runs parent") {
		t.Fatal("workflow dispatcher must not read mutable definition edges")
	}
}
