// Package workflow contains deterministic DAG validation. Persistence and
// dispatchers must call this before making a workflow definition active.
package workflow

import (
	"fmt"
	"sort"
)

type Node struct{ ID, Key string }
type Edge struct{ From, To string }

func Validate(nodes []Node, edges []Edge) ([]string, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("workflow needs at least one node")
	}
	ids := make(map[string]struct{}, len(nodes))
	keys := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if node.ID == "" || node.Key == "" {
			return nil, fmt.Errorf("workflow node id and key are required")
		}
		if _, exists := ids[node.ID]; exists {
			return nil, fmt.Errorf("duplicate workflow node id")
		}
		if _, exists := keys[node.Key]; exists {
			return nil, fmt.Errorf("duplicate workflow node key")
		}
		ids[node.ID], keys[node.Key] = struct{}{}, struct{}{}
	}
	in := make(map[string]int, len(nodes))
	next := make(map[string][]string, len(nodes))
	for _, edge := range edges {
		if edge.From == edge.To {
			return nil, fmt.Errorf("workflow cannot contain a self edge")
		}
		if _, ok := ids[edge.From]; !ok {
			return nil, fmt.Errorf("edge source does not exist")
		}
		if _, ok := ids[edge.To]; !ok {
			return nil, fmt.Errorf("edge target does not exist")
		}
		next[edge.From] = append(next[edge.From], edge.To)
		in[edge.To]++
	}
	ready := make([]string, 0, len(nodes))
	for id := range ids {
		if in[id] == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	order := make([]string, 0, len(nodes))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		order = append(order, id)
		sort.Strings(next[id])
		for _, to := range next[id] {
			in[to]--
			if in[to] == 0 {
				ready = append(ready, to)
			}
		}
		sort.Strings(ready)
	}
	if len(order) != len(nodes) {
		return nil, fmt.Errorf("workflow contains a cycle")
	}
	return order, nil
}
