package gascitypacks

import (
	"os"
	"strings"
	"testing"
)

func TestGastownWitnessLivenessUsesCurrentRosterSchemaAndDeduplicatesAlerts(t *testing.T) {
	body, err := os.ReadFile("gastown/formulas/mol-witness-patrol.toml")
	if err != nil { t.Fatal(err) }
	text := string(body)
	for _, want := range []string{`reduce (.sessions // [])[] as $s`, `LIVENESS_ALERT_EXISTS=`, `ephemeral=true AND status=open`} {
		if !strings.Contains(text, want) { t.Errorf("witness liveness procedure missing %q", want) }
	}
	if strings.Contains(text, `reduce ($sessions[0].sessions // [])[] as $s`) { t.Error("witness liveness parser still relies on the brittle slurpfile wrapper") }
}
