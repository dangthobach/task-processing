package workflow

import "testing"

func TestValidateRejectsCycle(t *testing.T) {
	_, err := Validate([]Node{{"a", "A"}, {"b", "B"}}, []Edge{{"a", "b"}, {"b", "a"}})
	if err == nil {
		t.Fatal("expected cycle failure")
	}
}
func TestValidateReturnsDeterministicOrder(t *testing.T) {
	order, err := Validate([]Node{{"b", "B"}, {"a", "A"}, {"c", "C"}}, []Edge{{"a", "c"}, {"b", "c"}})
	if err != nil || len(order) != 3 || order[0] != "a" || order[1] != "b" {
		t.Fatalf("order=%v err=%v", order, err)
	}
}
