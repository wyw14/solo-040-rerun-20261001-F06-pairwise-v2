package hydraulics

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Controller coordinates valve commands, level windows and ramp operations.
type Controller struct {
	mu       sync.RWMutex
	commands *gate.Commands
	recorder *journal.Recorder
	windows  map[string]*LevelWindow
	ramp     *Ramp
	guard    CycleGuard
	now      func() time.Time
}

func NewController(commands *gate.Commands, recorder *journal.Recorder, ramp *Ramp, now func() time.Time) *Controller {
	if now == nil {
		now = time.Now
	}
	controller := &Controller{
		commands: commands, recorder: recorder, windows: make(map[string]*LevelWindow),
		ramp: ramp, now: now,
	}
	if ramp != nil {
		ramp.SetPhaseHook(controller.recordTerminal)
	}
	return controller
}

// BindCycleGuard supplies the transit-generation check used before every step
// dispatch and every resume. It is wired after construction to avoid an import
// cycle with the cycle package.
func (c *Controller) BindCycleGuard(guard CycleGuard) {
	c.mu.Lock()
	c.guard = guard
	c.mu.Unlock()
}

func (c *Controller) cycleGuard() CycleGuard {
	c.mu.RLock()
	guard := c.guard
	c.mu.RUnlock()
	return guard
}

func (c *Controller) window(chamberID string) (*LevelWindow, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	window := c.windows[chamberID]
	if window != nil {
		return window, nil
	}
	created, err := NewLevelWindow(4)
	if err != nil {
		return nil, err
	}
	c.windows[chamberID] = created
	return created, nil
}

func (c *Controller) BeginLeveling(cycle model.TransitCycle) error {
	window, err := c.window(cycle.ChamberID)
	if err != nil {
		return err
	}
	window.Begin(cycle.ID, cycle.Generation)
	return nil
}

func (c *Controller) Observe(sample model.LevelSample) (bool, error) {
	window, err := c.windowForCycle(sample.CycleID, sample.Generation)
	if err != nil {
		return false, err
	}
	if err := window.Add(sample); err != nil {
		return false, err
	}
	return window.Stable(0.05), nil
}

func (c *Controller) windowForCycle(cycleID interface{ String() string }, generation uint64) (*LevelWindow, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, window := range c.windows {
		id, currentGeneration := window.Identity()
		if id.String() == cycleID.String() && currentGeneration == generation {
			return window, nil
		}
	}
	return nil, errors.New("no active level window for transit")
}

// IssueValve dispatches one valve opening and returns the field command id.
func (c *Controller) IssueValve(ctx context.Context, cycle model.TransitCycle, target float64) (uuid.UUID, error) {
	device := model.DeviceFillValve
	if target < 0 {
		device = model.DeviceDrainValve
		target = -target
	}
	command, err := c.commands.Issue(cycle, device, "set_opening", target, c.now())
	if err != nil {
		return uuid.Nil, err
	}
	if _, err := c.recorder.Record(ctx, cycle.ChamberID, model.EventCommandQueued, cycle.ID, cycle.Generation, command); err != nil {
		return uuid.Nil, err
	}
	return command.ID, nil
}

// StartRamp begins a background valve ramp for the transit. The returned view
// lists every planned step and the phase so the operator can track progress.
func (c *Controller) StartRamp(ctx context.Context, cycle model.TransitCycle, targets []float64) (RampView, error) {
	view, err := c.ramp.ActiveRamp(cycle, targets, c.IssueValve, c.cycleGuard())
	if err != nil {
		return RampView{}, err
	}
	if _, err := c.recorder.Record(ctx, cycle.ChamberID, model.EventRampStarted, cycle.ID, cycle.Generation, view); err != nil {
		return RampView{}, err
	}
	return view, nil
}

func (c *Controller) RampView(chamberID string) (RampView, bool) {
	return c.ramp.View(chamberID)
}

func (c *Controller) PauseRamp(ctx context.Context, chamberID string) (RampView, error) {
	view, err := c.ramp.Pause(chamberID)
	if err != nil {
		return RampView{}, err
	}
	_, _ = c.recorder.Record(ctx, chamberID, model.EventRampPaused, view.CycleID, view.Generation, view)
	return view, nil
}

// ResumeRamp continues after an explicit safety review. The resume token must
// come from the latest pause, and the transit generation must still be active.
func (c *Controller) ResumeRamp(ctx context.Context, chamberID string, token uuid.UUID) (RampView, error) {
	view, err := c.ramp.Resume(chamberID, token)
	if err != nil {
		return RampView{}, err
	}
	_, _ = c.recorder.Record(ctx, chamberID, model.EventRampResumed, view.CycleID, view.Generation, view)
	return view, nil
}

func (c *Controller) CancelRamp(ctx context.Context, chamberID, reason string) (RampView, error) {
	// The worker's terminal hook records ramp.cancelled exactly once (after
	// run.done), so this method only requests cancellation and waits.
	return c.ramp.Cancel(chamberID, reason)
}

// recordTerminal is invoked by the worker when a ramp reaches completed,
// cancelled or failed without an operator request in flight.
func (c *Controller) recordTerminal(view RampView) {
	kind := model.EventRampCompleted
	switch view.Phase {
	case PhaseCancelled:
		kind = model.EventRampCancelled
	case PhaseFailed:
		kind = model.EventRampFailed
	case PhaseCompleted:
		kind = model.EventRampCompleted
	default:
		return
	}
	// Journal writes must not depend on the cancelled HTTP request.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 5*time.Second)
	defer cancel()
	_, _ = c.recorder.Record(ctx, view.ChamberID, kind, view.CycleID, view.Generation, view)
}

func (c *Controller) WindowSamples(chamberID string) []model.LevelSample {
	c.mu.RLock()
	window := c.windows[chamberID]
	c.mu.RUnlock()
	if window == nil {
		return nil
	}
	return window.Samples()
}
