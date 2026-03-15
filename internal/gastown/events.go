// Package gastown emits events to Gas Town's event stream.
//
// When bd (beads CLI) runs inside a Gas Town deployment, this package
// appends bead lifecycle events (create, update, close) to the shared
// .events.jsonl file. This enables the GTP plugin, gt feed, and other
// consumers to observe bead changes in real time.
//
// Event emission is fire-and-forget: errors are logged via debug.Logf
// and never returned to the caller. If Gas Town is not detected (no
// GT_HOME, no ~/gt), events are silently skipped.
package gastown

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
	"github.com/steveyegge/beads/internal/debug"
	"github.com/steveyegge/beads/internal/types"
)

// Event types emitted by bd commands.
const (
	TypeBeadCreated = "bead_created"
	TypeBeadUpdated = "bead_updated"
	TypeBeadClosed  = "bead_closed"
)

// event matches gastown's internal/events.Event struct.
type event struct {
	Timestamp  string                 `json:"ts"`
	Source     string                 `json:"source"`
	Type       string                 `json:"type"`
	Actor      string                 `json:"actor"`
	Payload    map[string]interface{} `json:"payload,omitempty"`
	Visibility string                 `json:"visibility"`
}

// eventsFile is the name of gastown's raw events log.
const eventsFile = ".events.jsonl"

// EmitBeadCreated emits a bead_created event for a newly created issue.
func EmitBeadCreated(actor string, issue *types.Issue) {
	if issue == nil {
		return
	}
	emit(TypeBeadCreated, actor, beadPayload(issue))
}

// EmitBeadUpdated emits a bead_updated event for an updated issue.
func EmitBeadUpdated(actor string, issue *types.Issue) {
	if issue == nil {
		return
	}
	emit(TypeBeadUpdated, actor, beadPayload(issue))
}

// EmitBeadClosed emits a bead_closed event for a closed issue.
func EmitBeadClosed(actor string, issue *types.Issue) {
	if issue == nil {
		return
	}
	emit(TypeBeadClosed, actor, beadPayload(issue))
}

// beadPayload creates the event payload from an issue.
func beadPayload(issue *types.Issue) map[string]interface{} {
	p := map[string]interface{}{
		"id":       issue.ID,
		"title":    issue.Title,
		"type":     string(issue.IssueType),
		"priority": issue.Priority,
		"status":   string(issue.Status),
	}
	if issue.Assignee != "" {
		p["assignee"] = issue.Assignee
	}
	return p
}

// emit writes an event to gastown's .events.jsonl.
// Fire-and-forget: errors are debug-logged, never fatal.
func emit(eventType, actor string, payload map[string]interface{}) {
	eventsPath := findEventsFile()
	if eventsPath == "" {
		return // Not in a Gas Town deployment
	}

	ev := event{
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		Source:     "bd",
		Type:       eventType,
		Actor:      actor,
		Payload:    payload,
		Visibility: "feed",
	}

	data, err := json.Marshal(ev)
	if err != nil {
		debug.Logf("gastown: failed to marshal event: %v", err)
		return
	}
	data = append(data, '\n')

	// Acquire cross-process file lock (matches gastown's locking pattern)
	fl := flock.New(eventsPath + ".lock")
	if err := fl.Lock(); err != nil {
		debug.Logf("gastown: failed to acquire events lock: %v", err)
		return
	}
	defer fl.Unlock() //nolint:errcheck

	f, err := os.OpenFile(eventsPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644) //nolint:gosec // G302: events file is non-sensitive operational data
	if err != nil {
		debug.Logf("gastown: failed to open events file: %v", err)
		return
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		debug.Logf("gastown: failed to write event: %v", err)
		return
	}

	if err := f.Close(); err != nil {
		debug.Logf("gastown: failed to close events file: %v", err)
	}
}

// findEventsFile locates gastown's .events.jsonl file.
// Checks: GT_EVENTS env var → GT_HOME env var → ~/gt/.events.jsonl
func findEventsFile() string {
	// Direct path override (for testing)
	if p := os.Getenv("GT_EVENTS"); p != "" {
		return p
	}

	// GT_HOME points to the town root
	if home := os.Getenv("GT_HOME"); home != "" {
		p := filepath.Join(home, eventsFile)
		if _, err := os.Stat(filepath.Dir(p)); err == nil {
			return p
		}
	}

	// Default: ~/gt/.events.jsonl
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	gtDir := filepath.Join(homeDir, "gt")
	if info, err := os.Stat(gtDir); err == nil && info.IsDir() {
		return filepath.Join(gtDir, eventsFile)
	}

	return ""
}

// IsAvailable reports whether gastown event emission is possible.
// Can be used to skip event emission early if gastown is not present.
func IsAvailable() bool {
	return findEventsFile() != ""
}

// FormatActor formats a Gas Town actor string from available context.
// If GT_ACTOR is set (by gt sling/hook), use it. Otherwise fall back to
// the bd actor (typically user name or agent identity).
func FormatActor(bdActor string) string {
	if gtActor := os.Getenv("GT_ACTOR"); gtActor != "" {
		return gtActor
	}
	return fmt.Sprintf("bd/%s", bdActor)
}
