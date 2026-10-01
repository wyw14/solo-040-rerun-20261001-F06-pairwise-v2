package hydraulics

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// StepState tracks how far a single valve opening has progressed.
type StepState string

const (
	StepPending   StepState = "pending"   // not issued yet
	StepIssued    StepState = "issued"    // opening command dispatched
	StepCompleted StepState = "completed" // command dispatched and paced past
)

// RampPhase is the lifecycle state of one operator ramp.
type RampPhase string

const (
	PhaseRunning   RampPhase = "running"   // steps are being issued in sequence
	PhasePaused    RampPhase = "paused"    // suspended before the next pending step
	PhaseCompleted RampPhase = "completed" // every step completed
	PhaseCancelled RampPhase = "cancelled" // cancelled by an operator or a safety gate
	PhaseFailed    RampPhase = "failed"    // a step command could not be issued
)

func (p RampPhase) Terminal() bool {
	return p == PhaseCompleted || p == PhaseCancelled || p == PhaseFailed
}

// RampStep is one target opening inside a ramp plan.
type RampStep struct {
	Index       int        `json:"index"`
	Target      float64    `json:"target"`
	State       StepState  `json:"state"`
	IssuedAt    *time.Time `json:"issued_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	// CommandID links an issued step to its field command for audit/ack.
	CommandID uuid.UUID `json:"command_id,omitempty"`
}

// StepIssuer dispatches one valve opening and returns the command identity.
type StepIssuer func(context.Context, model.TransitCycle, float64) (uuid.UUID, error)

// CycleGuard re-validates the transit generation immediately before a step is
// issued or a ramp is resumed. It blocks continuing after cancellation, an
// emergency stop, or when the chamber has moved to a newer transit generation.
type CycleGuard func(model.TransitCycle) error

// ResumeToken is issued while a ramp is paused. Resumption requires the token
// from the latest pause together with a fresh safety review; a stale token is
// rejected.
type ResumeToken struct {
	Token     uuid.UUID `json:"token"`
	IssuedAt  time.Time `json:"issued_at"`
	NextIndex int       `json:"next_index"`
}

// RampView is the operator-visible snapshot of a ramp.
type RampView struct {
	ID         uuid.UUID    `json:"id"`
	ChamberID  string       `json:"chamber_id"`
	CycleID    uuid.UUID    `json:"cycle_id"`
	Generation uint64       `json:"generation"`
	Phase      RampPhase    `json:"phase"`
	Steps      []RampStep   `json:"steps"`
	Completed  int          `json:"completed_count"`
	Remaining  int          `json:"remaining_count"`
	NextIndex  *int         `json:"next_index,omitempty"`
	Reason     string       `json:"reason,omitempty"`
	CreatedAt  time.Time    `json:"created_at"`
	UpdatedAt  time.Time    `json:"updated_at"`
	Resume     *ResumeToken `json:"resume_token,omitempty"`
}

// Ramp executes valve targets as a pausable, resumable background plan. Every
// target is issued at most once: completed/issued steps are never
// re-dispatched, including across a pause/resume cycle.
type Ramp struct {
	mu           sync.Mutex
	interval     time.Duration
	now          func() time.Time
	active       *rampRun
	terminalHook func(RampView)
}

// SetPhaseHook registers a callback fired once when a ramp reaches a terminal
// phase, including worker-driven cancellation via the cycle guard.
func (r *Ramp) SetPhaseHook(hook func(RampView)) {
	r.mu.Lock()
	r.terminalHook = hook
	r.mu.Unlock()
}

func NewRamp(interval time.Duration, now func() time.Time) *Ramp {
	if interval <= 0 {
		interval = 20 * time.Millisecond
	}
	if now == nil {
		now = time.Now
	}
	return &Ramp{interval: interval, now: now}
}

type controlKind string

const (
	ctlPause  controlKind = "pause"
	ctlResume controlKind = "resume"
)

type control struct {
	kind  controlKind
	token uuid.UUID
	err   chan error
}

type rampRun struct {
	mu         sync.Mutex
	view       RampView
	steps      []RampStep
	cycle      model.TransitCycle
	issue      StepIssuer
	guard      CycleGuard
	now        func() time.Time
	ctx        context.Context
	controls   chan control
	stop       chan struct{}
	done       chan struct{}
	stopOnce   sync.Once
	finishOnce sync.Once
	cancelFn   context.CancelFunc
	onTerminal func(RampView)
	paused     bool
	token      *ResumeToken
}

// ActiveRamp starts a background ramp for the transit and returns its initial
// view. Only one non-terminal ramp per chamber may run at a time.
func (r *Ramp) ActiveRamp(cycle model.TransitCycle, targets []float64, issue StepIssuer, guard CycleGuard) (RampView, error) {
	if issue == nil {
		return RampView{}, errors.New("ramp requires a step issuer")
	}
	if len(targets) == 0 {
		return RampView{}, errors.New("ramp requires at least one target")
	}
	if err := guardCycle(guard, cycle); err != nil {
		return RampView{}, err
	}
	now := r.now().UTC()
	run := &rampRun{
		view: RampView{
			ID:         uuid.New(),
			ChamberID:  cycle.ChamberID,
			CycleID:    cycle.ID,
			Generation: cycle.Generation,
			Phase:      PhaseRunning,
			CreatedAt:  now,
			UpdatedAt:  now,
		},
		cycle:    cycle,
		issue:    issue,
		guard:    guard,
		now:      r.now,
		controls: make(chan control),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	run.steps = make([]RampStep, len(targets))
	for index, target := range targets {
		run.steps[index] = RampStep{Index: index, Target: target, State: StepPending}
	}
	run.view.Remaining = len(targets)
	first := 0
	run.view.NextIndex = &first

	r.mu.Lock()
	// snapshot() acquires the run's own mutex, which serializes phase writes
	// from the worker; the worker never takes the Ramp mutex back, so this lock
	// ordering cannot deadlock.
	if r.active != nil && !r.active.snapshot().Phase.Terminal() {
		r.mu.Unlock()
		return RampView{}, errors.New("a ramp is already active for this chamber")
	}
	run.onTerminal = r.terminalHook
	r.active = run
	r.mu.Unlock()

	run.ctx, run.cancelFn = context.WithCancel(context.Background())
	go run.loop(r.interval)
	return run.snapshot(), nil
}

func (run *rampRun) loop(interval time.Duration) {
	defer close(run.done)
	defer run.cancelFn()
	for {
		// Block here while paused; the worker goroutine is the sole reader of
		// the controls channel, so no pause signal can be left behind.
		for run.isPaused() {
			select {
			case <-run.stop:
				run.finish(PhaseCancelled, "operator cancelled the ramp")
				return
			case request := <-run.controls:
				if !run.serveWhilePaused(request) {
					return
				}
			}
		}
		select {
		case <-run.stop:
			run.finish(PhaseCancelled, "operator cancelled the ramp")
			return
		case request := <-run.controls:
			if request.kind == ctlPause && run.nextPending() < 0 {
				run.finish(PhaseCompleted, "")
				request.reply(nil)
				return
			}
			if !run.serveWhileRunning(request) {
				return
			}
			continue
		default:
		}

		index := run.nextPending()
		if index < 0 {
			run.finish(PhaseCompleted, "")
			return
		}
		// Re-validate the transit generation immediately before dispatch so a
		// step is never issued against a cancelled/emergency/superseded cycle.
		if err := guardCycle(run.guard, run.cycle); err != nil {
			run.finish(PhaseCancelled, err.Error())
			return
		}
		commandID, err := run.issue(run.ctx, run.cycle, run.steps[index].Target)
		if err != nil {
			select {
			case <-run.stop:
				run.finish(PhaseCancelled, "operator cancelled the ramp")
			default:
				run.finish(PhaseFailed, err.Error())
			}
			return
		}
		run.markIssued(index, commandID)

		// Pace dispatches, but stay responsive to pause and cancel.
		timer := time.NewTimer(interval)
		select {
		case <-run.stop:
			timer.Stop()
			run.finish(PhaseCancelled, "operator cancelled the ramp")
			return
		case request := <-run.controls:
			timer.Stop()
			if request.kind == ctlPause {
				// The dispatched step is allowed to settle; resumption
				// continues from the next pending step.
				run.markCompleted(index)
				if run.nextPending() < 0 {
					run.finish(PhaseCompleted, "")
					request.reply(nil)
					return
				}
				run.enterPaused()
				request.reply(nil)
				continue
			}
			request.reply(errors.New("ramp is not paused"))
		case <-timer.C:
		}
		run.markCompleted(index)
	}
}

// serveWhilePaused handles controls received in the paused wait loop. It
// reports false when the worker must exit.
func (run *rampRun) serveWhilePaused(request control) bool {
	switch request.kind {
	case ctlPause:
		request.reply(nil) // idempotent
		return true
	case ctlResume:
		token := run.currentToken()
		if token == nil || request.token != token.Token {
			request.reply(errors.New("resume confirmation is stale; review the current ramp and retry"))
			return true
		}
		if err := guardCycle(run.guard, run.cycle); err != nil {
			run.finish(PhaseCancelled, err.Error())
			request.reply(err)
			return false
		}
		run.exitPaused()
		request.reply(nil)
		return true
	default:
		request.reply(errors.New("unknown ramp control"))
		return true
	}
}

// serveWhileRunning handles a control observed between two steps.
func (run *rampRun) serveWhileRunning(request control) bool {
	if request.kind == ctlPause {
		run.enterPaused()
		request.reply(nil)
		return true
	}
	request.reply(errors.New("ramp is not paused"))
	return true
}

func (request control) reply(err error) {
	if request.err != nil {
		request.err <- err
	}
}

func (run *rampRun) isPaused() bool {
	run.mu.Lock()
	defer run.mu.Unlock()
	return run.paused
}

func (run *rampRun) nextPending() int {
	run.mu.Lock()
	defer run.mu.Unlock()
	if next := run.firstPendingLocked(); next < len(run.steps) {
		return next
	}
	return -1
}

func (run *rampRun) markIssued(index int, commandID uuid.UUID) {
	run.mu.Lock()
	defer run.mu.Unlock()
	now := run.now().UTC()
	run.steps[index].State = StepIssued
	run.steps[index].IssuedAt = &now
	run.steps[index].CommandID = commandID
	run.touchLocked()
}

func (run *rampRun) markCompleted(index int) {
	run.mu.Lock()
	defer run.mu.Unlock()
	if run.steps[index].State == StepCompleted {
		return
	}
	now := run.now().UTC()
	run.steps[index].State = StepCompleted
	run.steps[index].CompletedAt = &now
	run.touchLocked()
}

func (run *rampRun) enterPaused() {
	run.mu.Lock()
	defer run.mu.Unlock()
	if run.paused {
		return
	}
	run.paused = true
	run.view.Phase = PhasePaused
	now := run.now().UTC()
	run.token = &ResumeToken{Token: uuid.New(), IssuedAt: now, NextIndex: run.firstPendingLocked()}
	run.touchLocked()
}

func (run *rampRun) exitPaused() {
	run.mu.Lock()
	defer run.mu.Unlock()
	run.paused = false
	run.token = nil
	run.view.Phase = PhaseRunning
	run.touchLocked()
}

func (run *rampRun) firstPendingLocked() int {
	for index := range run.steps {
		if run.steps[index].State == StepPending {
			return index
		}
	}
	return len(run.steps)
}

func (run *rampRun) touchLocked() {
	run.view.UpdatedAt = run.now().UTC()
}

func (run *rampRun) currentToken() *ResumeToken {
	run.mu.Lock()
	defer run.mu.Unlock()
	if run.token == nil {
		return nil
	}
	copy := *run.token
	return &copy
}

// finish is idempotent so cancel paths and normal completion cannot overwrite
// each other. It cancels the dispatch context so an in-flight issuer unwinds.
func (run *rampRun) finish(phase RampPhase, reason string) {
	run.finishOnce.Do(func() {
		run.mu.Lock()
		run.paused = false
		run.view.Phase = phase
		run.view.Reason = reason
		run.view.UpdatedAt = run.now().UTC()
		view := run.snapshotLocked()
		run.mu.Unlock()
		run.cancelFn()
		if run.onTerminal != nil {
			run.onTerminal(view)
		}
	})
}

func (run *rampRun) snapshotLocked() RampView {
	view := run.view
	view.Steps = append([]RampStep(nil), run.steps...)
	completed := 0
	for _, step := range run.steps {
		if step.State == StepCompleted {
			completed++
		}
	}
	view.Completed = completed
	view.Remaining = len(run.steps) - completed
	if next := run.firstPendingLocked(); next < len(run.steps) {
		index := next
		view.NextIndex = &index
	} else {
		view.NextIndex = nil
	}
	if run.token != nil && run.paused {
		token := *run.token
		view.Resume = &token
	}
	return view
}

func (run *rampRun) snapshot() RampView {
	run.mu.Lock()
	defer run.mu.Unlock()
	return run.snapshotLocked()
}

func guardCycle(guard CycleGuard, cycle model.TransitCycle) error {
	if guard == nil {
		return nil
	}
	return guard(cycle)
}

// View returns the ramp most recently started for the chamber, including a
// terminal one kept for operator inspection.
func (r *Ramp) View(chamberID string) (RampView, bool) {
	r.mu.Lock()
	run := r.active
	r.mu.Unlock()
	if run == nil || run.view.ChamberID != chamberID {
		return RampView{}, false
	}
	return run.snapshot(), true
}

// Pause suspends the ramp before its next pending step. It blocks until the
// worker acknowledges and is idempotent for an already paused ramp.
func (r *Ramp) Pause(chamberID string) (RampView, error) {
	run, err := r.requireRamp(chamberID)
	if err != nil {
		return RampView{}, err
	}
	if view := run.snapshot(); view.Phase.Terminal() {
		return view, errors.New("ramp is already finished")
	}
	return run.send(control{kind: ctlPause, err: make(chan error, 1)})
}

// Resume continues a paused ramp from its next pending step after a safety
// review. The token must match the latest pause and the guard must still accept
// the transit generation.
func (r *Ramp) Resume(chamberID string, token uuid.UUID) (RampView, error) {
	run, err := r.requireRamp(chamberID)
	if err != nil {
		return RampView{}, err
	}
	if view := run.snapshot(); view.Phase.Terminal() {
		return view, errors.New("ramp is already finished")
	}
	return run.send(control{kind: ctlResume, token: token, err: make(chan error, 1)})
}

// Cancel stops a running or paused ramp. Steps already issued are not re-sent.
func (r *Ramp) Cancel(chamberID, reason string) (RampView, error) {
	run, err := r.requireRamp(chamberID)
	if err != nil {
		return RampView{}, err
	}
	view := run.snapshot()
	if view.Phase.Terminal() {
		return view, errors.New("ramp is already finished")
	}
	if reason != "" {
		run.mu.Lock()
		run.view.Reason = reason
		run.view.UpdatedAt = run.now().UTC()
		run.mu.Unlock()
	}
	// Closing stop releases the worker from a pause/timer wait; cancelling the
	// dispatch context unwinds an in-flight issuer.
	run.stopOnce.Do(func() { close(run.stop) })
	run.cancelFn()
	<-run.done
	return run.snapshot(), nil
}

func (run *rampRun) send(request control) (RampView, error) {
	select {
	case run.controls <- request:
	case <-run.done:
		return run.snapshot(), errors.New("ramp stopped before request was handled")
	}
	// Drain a ready reply first so a concurrent finish (e.g. guard rejection
	// on resume) surfaces its specific reason instead of a generic stop error.
	select {
	case err := <-request.err:
		if err != nil {
			return run.snapshot(), err
		}
		return run.snapshot(), nil
	default:
	}
	select {
	case err := <-request.err:
		if err != nil {
			return run.snapshot(), err
		}
	case <-run.done:
		// The worker may have answered just before finishing; a buffered reply
		// still carries the specific reason.
		select {
		case err := <-request.err:
			if err != nil {
				return run.snapshot(), err
			}
		default:
			return run.snapshot(), errors.New("ramp stopped before request was handled")
		}
	}
	return run.snapshot(), nil
}

func (r *Ramp) requireRamp(chamberID string) (*rampRun, error) {
	r.mu.Lock()
	run := r.active
	r.mu.Unlock()
	if run == nil || run.view.ChamberID != chamberID {
		return nil, errors.New("no ramp for this chamber")
	}
	return run, nil
}
