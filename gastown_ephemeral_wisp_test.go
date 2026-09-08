package gascitypacks

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGastownPatrolReconciliationQueriesEphemeralWisps prevents patrol
// restarts from leaking wisps. The bd list projection excludes ephemeral
// molecule roots, so each patrol reconciliation must use an ephemeral query.
func TestGastownPatrolReconciliationQueriesEphemeralWisps(t *testing.T) {
	files := []string{
		"gastown/agents/deacon/prompt.template.md",
		"gastown/agents/refinery/prompt.template.md",
		"gastown/agents/witness/prompt.template.md",
		"gastown/formulas/mol-deacon-patrol.toml",
		"gastown/formulas/mol-refinery-patrol.toml",
		"gastown/formulas/mol-witness-patrol.toml",
	}

	for _, name := range files {
		data, err := os.ReadFile(filepath.FromSlash(name))
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		body := string(data)
		if strings.Contains(body, `gc bd list --assignee="$GC_AGENT"`) && strings.Contains(body, "--type=molecule") {
			t.Errorf("%s reconciles patrol wisps with gc bd list, which excludes ephemeral roots", name)
		}
		if !strings.Contains(body, `gc bd query --json 'ephemeral=true AND status=`) {
			t.Errorf("%s does not query ephemeral patrol wisps", name)
		}
	}
}
