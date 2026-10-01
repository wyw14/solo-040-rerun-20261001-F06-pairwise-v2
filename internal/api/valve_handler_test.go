package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/cry-96/internal/model"
)

type rampAPIView struct {
	ID         string `json:"id"`
	CycleID    string `json:"cycle_id"`
	Generation uint64 `json:"generation"`
	Phase      string `json:"phase"`
	Steps      []struct {
		Index     int     `json:"index"`
		Target    float64 `json:"target"`
		State     string  `json:"state"`
		CommandID string  `json:"command_id"`
	} `json:"steps"`
	Completed int  `json:"completed_count"`
	Remaining int  `json:"remaining_count"`
	NextIndex *int `json:"next_index"`
	Resume    *struct {
		Token string `json:"token"`
	} `json:"resume_token"`
	Reason string `json:"reason"`
}

func newTestServer(t *testing.T, interval time.Duration) (*Runtime, *httptest.Server) {
	t.Helper()
	runtime, err := NewRuntime(context.Background(), "", WithRampInterval(interval))
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	server := httptest.NewServer(Router(runtime, "."))
	t.Cleanup(server.Close)
	return runtime, server
}

func doJSON(t *testing.T, method, url string, body any) (int, []byte) {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		encoded, _ := json.Marshal(body)
		reader = bytes.NewReader(encoded)
	} else {
		reader = bytes.NewReader(nil)
	}
	request, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("do %s %s: %v", method, url, err)
	}
	defer response.Body.Close()
	buffer := new(bytes.Buffer)
	_, _ = buffer.ReadFrom(response.Body)
	return response.StatusCode, buffer.Bytes()
}

func decodeRamp(t *testing.T, raw []byte) rampAPIView {
	t.Helper()
	var view rampAPIView
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatalf("decode ramp view: %v\n%s", err, raw)
	}
	return view
}

func createPlanHTTP(t *testing.T, server *httptest.Server, chamber, vessel string) (planID, cycleID string, generation uint64) {
	t.Helper()
	status, raw := doJSON(t, http.MethodPost, server.URL+"/api/plans", map[string]any{
		"chamber_id": chamber,
		"direction":  "upstream",
		"vessels":    []map[string]any{{"name": vessel, "call_sign": strings.ToUpper(vessel), "length_m": 80}},
	})
	if status != http.StatusCreated {
		t.Fatalf("create plan status=%d body=%s", status, raw)
	}
	var response struct {
		Plan struct {
			ID string `json:"id"`
		} `json:"plan"`
		Cycle struct {
			ID         string `json:"id"`
			Generation uint64 `json:"generation"`
		} `json:"cycle"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatalf("decode plan: %v", err)
	}
	return response.Plan.ID, response.Cycle.ID, response.Cycle.Generation
}

func rampURL(server *httptest.Server, chamber string) string {
	return server.URL + "/api/chambers/" + chamber + "/valves/ramp"
}

func waitPhase(t *testing.T, server *httptest.Server, chamber string, phases ...string) rampAPIView {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		status, raw := doJSON(t, http.MethodGet, rampURL(server, chamber), nil)
		if status == http.StatusOK {
			view := decodeRamp(t, raw)
			for _, phase := range phases {
				if view.Phase == phase {
					return view
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("ramp never reached %v", phases)
	return rampAPIView{}
}

// pauseRamp retries pause until the ramp settles into the paused phase,
// returning its current resume token.
func pauseRamp(t *testing.T, server *httptest.Server, chamber string) rampAPIView {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		status, raw := doJSON(t, http.MethodPost, rampURL(server, chamber)+"/pause", map[string]any{})
		if status == http.StatusOK {
			view := decodeRamp(t, raw)
			if view.Phase == "paused" && view.Resume != nil {
				return view
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("ramp did not pause")
	return rampAPIView{}
}

func makeTargets(n int) []float64 {
	targets := make([]float64, n)
	for index := range targets {
		targets[index] = float64(index+1) * 0.1
	}
	return targets
}

func TestValveRampStartTracksStepsAndCompletesOnce(t *testing.T) {
	runtime, server := newTestServer(t, 15*time.Millisecond)
	createPlanHTTP(t, server, "chamber-1", "River Star")

	status, raw := doJSON(t, http.MethodPost, rampURL(server, "chamber-1"), map[string]any{"targets": []float64{0.1, 0.2, 0.3, 0.4}})
	if status != http.StatusAccepted {
		t.Fatalf("start status=%d body=%s", status, raw)
	}
	view := decodeRamp(t, raw)
	if view.Phase != "running" || len(view.Steps) != 4 || view.Completed != 0 {
		t.Fatalf("unexpected initial view: phase=%s steps=%d completed=%d", view.Phase, len(view.Steps), view.Completed)
	}
	if status, raw = doJSON(t, http.MethodGet, rampURL(server, "chamber-1"), nil); status != http.StatusOK {
		t.Fatalf("view status=%d body=%s", status, raw)
	}

	completed := waitPhase(t, server, "chamber-1", "completed")
	if completed.Completed != 4 || completed.Remaining != 0 {
		t.Fatalf("completed=%d remaining=%d", completed.Completed, completed.Remaining)
	}
	seen := map[string]bool{}
	for _, step := range completed.Steps {
		if step.State != "completed" || step.CommandID == "" {
			t.Fatalf("step %d not completed/linked: %+v", step.Index, step)
		}
		if seen[step.CommandID] {
			t.Fatalf("command %s reused for two openings", step.CommandID)
		}
		seen[step.CommandID] = true
	}
	// Every opening produced exactly one field command and none were re-sent.
	if pending := runtime.GateCommands.Pending("chamber-1"); len(pending) != 4 {
		t.Fatalf("expected 4 queued valve commands, got %d", len(pending))
	}
}

func TestValveRampRequiresActiveTransit(t *testing.T) {
	_, server := newTestServer(t, 15*time.Millisecond)
	status, raw := doJSON(t, http.MethodPost, rampURL(server, "chamber-2"), map[string]any{"targets": []float64{0.1}})
	if status != http.StatusConflict {
		t.Fatalf("status=%d body=%s, want 409 without an active transit", status, raw)
	}
}

func TestValveRampResumeRequiresFreshSafetyReview(t *testing.T) {
	_, server := newTestServer(t, 50*time.Millisecond)
	createPlanHTTP(t, server, "chamber-1", "River Star")
	if status, raw := doJSON(t, http.MethodPost, rampURL(server, "chamber-1"), map[string]any{"targets": makeTargets(12)}); status != http.StatusAccepted {
		t.Fatalf("start status=%d body=%s", status, raw)
	}
	paused := pauseRamp(t, server, "chamber-1")
	token := paused.Resume.Token

	// Missing review confirmation.
	if status, _ := doJSON(t, http.MethodPost, rampURL(server, "chamber-1")+"/resume", map[string]any{
		"resume_token": token, "safety_review": false, "reviewer": "ops",
	}); status != http.StatusBadRequest {
		t.Fatalf("resume without review status=%d, want 400", status)
	}
	// Missing reviewer identity.
	if status, _ := doJSON(t, http.MethodPost, rampURL(server, "chamber-1")+"/resume", map[string]any{
		"resume_token": token, "safety_review": true, "reviewer": "   ",
	}); status != http.StatusBadRequest {
		t.Fatalf("resume without reviewer status=%d, want 400", status)
	}
	// A stale token from an older pause must not continue the ramp.
	if status, raw := doJSON(t, http.MethodPost, rampURL(server, "chamber-1")+"/resume", map[string]any{
		"resume_token": "00000000-0000-0000-0000-000000000000", "safety_review": true, "reviewer": "ops",
	}); status != http.StatusConflict {
		t.Fatalf("resume with stale token status=%d body=%s, want 409", status, raw)
	}
	still := waitPhase(t, server, "chamber-1", "paused")
	if still.Resume == nil || still.Resume.Token != token {
		t.Fatal("ramp should remain paused with its token after a rejected resume")
	}

	// Fresh review with the current token resumes from the next pending step.
	if status, raw := doJSON(t, http.MethodPost, rampURL(server, "chamber-1")+"/resume", map[string]any{
		"resume_token": token, "safety_review": true, "reviewer": "ops-lead",
	}); status != http.StatusOK {
		t.Fatalf("resume status=%d body=%s, want 200", status, raw)
	}
	completed := waitPhase(t, server, "chamber-1", "completed")
	if completed.Completed != 12 {
		t.Fatalf("expected all 12 openings after resume, completed=%d", completed.Completed)
	}
}

func TestValveRampRejectsResumeAfterCancellation(t *testing.T) {
	_, server := newTestServer(t, 50*time.Millisecond)
	planID, _, _ := createPlanHTTP(t, server, "chamber-1", "River Star")
	if status, raw := doJSON(t, http.MethodPost, rampURL(server, "chamber-1"), map[string]any{"targets": makeTargets(12)}); status != http.StatusAccepted {
		t.Fatalf("start status=%d body=%s", status, raw)
	}
	paused := pauseRamp(t, server, "chamber-1")

	// The transit (闸次) is cancelled while the ramp is paused.
	if status, raw := doJSON(t, http.MethodPost, server.URL+"/api/plans/"+planID+"/cancel", map[string]any{}); status != http.StatusOK {
		t.Fatalf("cancel plan status=%d body=%s", status, raw)
	}
	status, raw := doJSON(t, http.MethodPost, rampURL(server, "chamber-1")+"/resume", map[string]any{
		"resume_token": paused.Resume.Token, "safety_review": true, "reviewer": "ops",
	})
	if status != http.StatusConflict {
		t.Fatalf("resume after transit cancellation status=%d body=%s, want 409", status, raw)
	}
	view := waitPhase(t, server, "chamber-1", "cancelled")
	if view.Phase != "cancelled" || view.Completed >= 12 {
		t.Fatalf("ramp should stop cancelled with steps unissued, phase=%s completed=%d", view.Phase, view.Completed)
	}
}

func TestValveRampRejectsResumeAfterGenerationChange(t *testing.T) {
	runtime, server := newTestServer(t, 50*time.Millisecond)
	createPlanHTTP(t, server, "chamber-1", "River Star")
	if status, raw := doJSON(t, http.MethodPost, rampURL(server, "chamber-1"), map[string]any{"targets": makeTargets(12)}); status != http.StatusAccepted {
		t.Fatalf("start status=%d body=%s", status, raw)
	}
	paused := pauseRamp(t, server, "chamber-1")

	// Cancel the first transit, then open a newer generation on the chamber.
	if _, err := runtime.Cycles.Cancel(context.Background(), "chamber-1", "operator"); err != nil {
		t.Fatalf("cancel first cycle: %v", err)
	}
	vessel, err := model.NewVessel("Second Wave", "SW-900", 70)
	if err != nil {
		t.Fatalf("vessel: %v", err)
	}
	planB, err := model.NewConvoyPlan("chamber-1", model.DirectionUpstream, []model.Vessel{vessel}, runtime.Now())
	if err != nil {
		t.Fatalf("plan b: %v", err)
	}
	cycleB, err := runtime.Cycles.Start(context.Background(), planB)
	if err != nil {
		t.Fatalf("start second cycle: %v", err)
	}
	if cycleB.Generation != 2 {
		t.Fatalf("expected second generation, got %d", cycleB.Generation)
	}

	status, raw := doJSON(t, http.MethodPost, rampURL(server, "chamber-1")+"/resume", map[string]any{
		"resume_token": paused.Resume.Token, "safety_review": true, "reviewer": "ops",
	})
	if status != http.StatusConflict || !strings.Contains(string(raw), "generation") {
		t.Fatalf("resume after generation change status=%d body=%s, want 409 about generation", status, raw)
	}
	view := waitPhase(t, server, "chamber-1", "cancelled")
	if view.Generation != paused.Generation {
		t.Fatalf("terminal ramp view lost its generation binding: %d vs %d", view.Generation, paused.Generation)
	}
}

func TestValveRampCancelThenRestartDoesNotReissueOpenings(t *testing.T) {
	runtime, server := newTestServer(t, 50*time.Millisecond)
	createPlanHTTP(t, server, "chamber-1", "River Star")
	if status, raw := doJSON(t, http.MethodPost, rampURL(server, "chamber-1"), map[string]any{"targets": makeTargets(12)}); status != http.StatusAccepted {
		t.Fatalf("start status=%d body=%s", status, raw)
	}
	time.Sleep(30 * time.Millisecond)
	status, raw := doJSON(t, http.MethodPost, rampURL(server, "chamber-1")+"/cancel", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("cancel status=%d body=%s", status, raw)
	}
	cancelled := decodeRamp(t, raw)
	if cancelled.Phase != "cancelled" {
		t.Fatalf("phase=%s, want cancelled", cancelled.Phase)
	}
	// Every step that was issued or completed produced a field command; no
	// pending step may have been dispatched.
	dispatched := 0
	for _, step := range cancelled.Steps {
		if step.State != "pending" {
			dispatched++
		}
	}
	commandsAfterFirst := len(runtime.GateCommands.Pending("chamber-1"))
	if commandsAfterFirst != dispatched {
		t.Fatalf("issued commands=%d but dispatched steps=%d", commandsAfterFirst, dispatched)
	}

	// Resuming a cancelled ramp is impossible.
	if status, _ := doJSON(t, http.MethodPost, rampURL(server, "chamber-1")+"/resume", map[string]any{
		"resume_token": "00000000-0000-0000-0000-000000000000", "safety_review": true, "reviewer": "ops",
	}); status != http.StatusConflict {
		t.Fatalf("resume cancelled ramp status=%d, want 409", status)
	}

	// A fresh ramp may start after cancellation, replacing the terminal one.
	status, raw = doJSON(t, http.MethodPost, rampURL(server, "chamber-1"), map[string]any{"targets": []float64{0.1, 0.2}})
	if status != http.StatusAccepted {
		t.Fatalf("restart status=%d body=%s", status, raw)
	}
	completed := waitPhase(t, server, "chamber-1", "completed")
	if completed.Completed != 2 {
		t.Fatalf("fresh ramp completed=%d, want 2", completed.Completed)
	}
	// First ramp openings plus the two new ones, with no duplicates.
	if pending := runtime.GateCommands.Pending("chamber-1"); len(pending) != commandsAfterFirst+2 {
		t.Fatalf("expected %d total commands, got %d", commandsAfterFirst+2, len(pending))
	}
}
