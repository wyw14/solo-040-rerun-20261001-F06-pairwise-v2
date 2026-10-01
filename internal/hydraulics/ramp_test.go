package hydraulics

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

var errCycleStale = errors.New("transit is cancelled")

type recordingIssuer struct {
	mu      sync.Mutex
	targets []float64
	err     error
}

func testCycle() model.TransitCycle {
	cycle, err := model.NewTransitCycle("chamber-1", uuid.New(), model.DirectionUpstream, 1, time.Now())
	if err != nil {
		panic(err)
	}
	return cycle
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func issuerFunc(issuer *recordingIssuer) StepIssuer {
	return func(ctx context.Context, _ model.TransitCycle, target float64) (uuid.UUID, error) {
		select {
		case <-ctx.Done():
			return uuid.Nil, ctx.Err()
		default:
		}
		issuer.mu.Lock()
		defer issuer.mu.Unlock()
		if issuer.err != nil {
			return uuid.Nil, issuer.err
		}
		issuer.targets = append(issuer.targets, target)
		return uuid.New(), nil
	}
}

func TestRampCompletesAndIssuesEachOpeningOnce(t *testing.T) {
	ramp := NewRamp(5*time.Millisecond, nil)
	issuer := &recordingIssuer{}
	targets := []float64{0.1, 0.2, 0.3, 0.4}
	view, err := ramp.ActiveRamp(testCycle(), targets, issuerFunc(issuer), nil)
	if err != nil {
		t.Fatalf("start ramp: %v", err)
	}
	waitFor(t, time.Second, func() bool {
		current, ok := ramp.View(view.ChamberID)
		return ok && current.Phase == PhaseCompleted
	})
	current, _ := ramp.View(view.ChamberID)
	if current.Completed != len(targets) || current.Remaining != 0 {
		t.Fatalf("expected all %d steps completed, got completed=%d remaining=%d", len(targets), current.Completed, current.Remaining)
	}
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	if len(issuer.targets) != len(targets) {
		t.Fatalf("expected %d dispatches, got %d (%v)", len(targets), len(issuer.targets), issuer.targets)
	}
	for index, target := range targets {
		if issuer.targets[index] != target {
			t.Fatalf("dispatch %d = %v, want %v", index, issuer.targets[index], target)
		}
	}
}

func TestRampPauseResumeContinuesFromNextStepWithoutReissue(t *testing.T) {
	ramp := NewRamp(40*time.Millisecond, nil)
	issuer := &recordingIssuer{}
	targets := []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8}
	view, err := ramp.ActiveRamp(testCycle(), targets, issuerFunc(issuer), nil)
	if err != nil {
		t.Fatalf("start ramp: %v", err)
	}

	// Let at least one step complete, then pause before the next dispatch.
	waitFor(t, time.Second, func() bool {
		current, _ := ramp.View(view.ChamberID)
		return current.Completed >= 1
	})
	paused, err := ramp.Pause(view.ChamberID)
	if err != nil {
		t.Fatalf("pause: %v", err)
	}
	if paused.Phase != PhasePaused || paused.Resume == nil {
		t.Fatalf("expected paused view with resume token, got phase=%s token=%v", paused.Phase, paused.Resume)
	}
	if paused.NextIndex == nil || *paused.NextIndex < 1 {
		t.Fatalf("expected to resume at step >= 1, got %v", paused.NextIndex)
	}
	issuer.mu.Lock()
	issuedAtPause := len(issuer.targets)
	issuer.mu.Unlock()
	if issuedAtPause < 1 || issuedAtPause >= len(targets) {
		t.Fatalf("paused at unexpected dispatch count %d", issuedAtPause)
	}

	// A stale/unknown confirmation must not resume the ramp.
	if _, err := ramp.Resume(view.ChamberID, uuid.New()); err == nil {
		t.Fatal("resume with an unknown token should be rejected")
	}
	still, _ := ramp.View(view.ChamberID)
	if still.Phase != PhasePaused {
		t.Fatalf("ramp left paused phase after stale resume: %s", still.Phase)
	}

	// Nothing new should be dispatched while paused.
	time.Sleep(100 * time.Millisecond)
	issuer.mu.Lock()
	issuedWhilePaused := len(issuer.targets)
	issuer.mu.Unlock()
	if issuedWhilePaused != issuedAtPause {
		t.Fatalf("dispatch continued while paused: %d -> %d", issuedAtPause, issuedWhilePaused)
	}

	if _, err := ramp.Resume(view.ChamberID, paused.Resume.Token); err != nil {
		t.Fatalf("resume: %v", err)
	}
	waitFor(t, time.Second, func() bool {
		current, _ := ramp.View(view.ChamberID)
		return current.Phase == PhaseCompleted
	})

	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	if len(issuer.targets) != len(targets) {
		t.Fatalf("expected %d total dispatches, got %d (%v)", len(targets), len(issuer.targets), issuer.targets)
	}
	seen := map[float64]int{}
	for _, target := range issuer.targets {
		seen[target]++
	}
	for _, target := range targets {
		if seen[target] != 1 {
			t.Fatalf("target %v issued %d times, want exactly once", target, seen[target])
		}
	}
}

func TestRampBlocksResumeWhenGuardRejects(t *testing.T) {
	var guardMu sync.Mutex
	guardFails := false
	guard := func(model.TransitCycle) error {
		guardMu.Lock()
		defer guardMu.Unlock()
		if guardFails {
			return errCycleStale
		}
		return nil
	}
	ramp := NewRamp(40*time.Millisecond, nil)
	issuer := &recordingIssuer{}
	targets := []float64{0.1, 0.2, 0.3, 0.4}
	view, err := ramp.ActiveRamp(testCycle(), targets, issuerFunc(issuer), guard)
	if err != nil {
		t.Fatalf("start ramp: %v", err)
	}
	waitFor(t, time.Second, func() bool {
		current, _ := ramp.View(view.ChamberID)
		return current.Completed >= 1
	})
	paused, err := ramp.Pause(view.ChamberID)
	if err != nil {
		t.Fatalf("pause: %v", err)
	}

	// Simulate the cycle becoming terminal (cancelled/emergency) or a newer
	// transit generation taking over.
	guardMu.Lock()
	guardFails = true
	guardMu.Unlock()

	if _, err := ramp.Resume(view.ChamberID, paused.Resume.Token); err == nil {
		t.Fatal("resume should be rejected when the transit is no longer active")
	}
	waitFor(t, time.Second, func() bool {
		current, _ := ramp.View(view.ChamberID)
		return current.Phase == PhaseCancelled
	})
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	if len(issuer.targets) >= len(targets) {
		t.Fatalf("guard did not stop dispatch: issued %v", issuer.targets)
	}
	time.Sleep(60 * time.Millisecond)
	if count := len(issuer.targets); count >= len(targets) {
		t.Fatalf("step issued after guard rejection: %v", issuer.targets)
	}
}

func TestRampGuardStopsInFlightDispatch(t *testing.T) {
	var guardMu sync.Mutex
	// Guard is consulted once at start and before every dispatch: permit the
	// start plus the first two steps, then reject the third.
	allowed := 3
	guard := func(model.TransitCycle) error {
		guardMu.Lock()
		defer guardMu.Unlock()
		if allowed == 0 {
			return errCycleStale
		}
		allowed--
		return nil
	}
	ramp := NewRamp(5*time.Millisecond, nil)
	issuer := &recordingIssuer{}
	targets := []float64{0.1, 0.2, 0.3, 0.4}
	view, err := ramp.ActiveRamp(testCycle(), targets, issuerFunc(issuer), guard)
	if err != nil {
		t.Fatalf("start ramp: %v", err)
	}
	waitFor(t, time.Second, func() bool {
		current, _ := ramp.View(view.ChamberID)
		return current.Phase == PhaseCancelled
	})
	current, _ := ramp.View(view.ChamberID)
	for index, step := range current.Steps {
		if index < 2 {
			if step.State != StepCompleted {
				t.Fatalf("step %d state=%s, want completed", index, step.State)
			}
		} else if step.State != StepPending {
			t.Fatalf("step %d state=%s, want pending", index, step.State)
		}
	}
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	if len(issuer.targets) != 2 {
		t.Fatalf("expected exactly 2 dispatches, got %v", issuer.targets)
	}
}

func TestRampCancelStopsDispatchAndTerminalHookFiresOnce(t *testing.T) {
	ramp := NewRamp(40*time.Millisecond, nil)
	issuer := &recordingIssuer{}
	targets := []float64{0.1, 0.2, 0.3, 0.4, 0.5}
	view, err := ramp.ActiveRamp(testCycle(), targets, issuerFunc(issuer), nil)
	if err != nil {
		t.Fatalf("start ramp: %v", err)
	}
	waitFor(t, time.Second, func() bool {
		current, _ := ramp.View(view.ChamberID)
		return current.Completed >= 1
	})
	cancelled, err := ramp.Cancel(view.ChamberID, "operator test cancel")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if cancelled.Phase != PhaseCancelled {
		t.Fatalf("phase=%s, want cancelled", cancelled.Phase)
	}
	issuer.mu.Lock()
	issuedAtCancel := len(issuer.targets)
	issuer.mu.Unlock()
	time.Sleep(80 * time.Millisecond)
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	if len(issuer.targets) != issuedAtCancel {
		t.Fatalf("dispatch continued after cancel: %d -> %d", issuedAtCancel, len(issuer.targets))
	}
	if _, err := ramp.Cancel(view.ChamberID, "again"); err == nil {
		t.Fatal("cancelling a terminal ramp should fail")
	}
}

func TestRampTerminalHookFiresExactlyOnce(t *testing.T) {
	ramp := NewRamp(5*time.Millisecond, nil)
	var hookMu sync.Mutex
	hooks := 0
	ramp.SetPhaseHook(func(RampView) {
		hookMu.Lock()
		hooks++
		hookMu.Unlock()
	})
	issuer := &recordingIssuer{}
	view, err := ramp.ActiveRamp(testCycle(), []float64{0.1, 0.2}, issuerFunc(issuer), nil)
	if err != nil {
		t.Fatalf("start ramp: %v", err)
	}
	waitFor(t, time.Second, func() bool {
		current, _ := ramp.View(view.ChamberID)
		return current.Phase == PhaseCompleted
	})
	time.Sleep(20 * time.Millisecond)
	hookMu.Lock()
	defer hookMu.Unlock()
	if hooks != 1 {
		t.Fatalf("terminal hook fired %d times, want 1", hooks)
	}
}
