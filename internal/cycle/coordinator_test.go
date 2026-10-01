package cycle

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

func guardCoordinator(cycle model.TransitCycle) *Coordinator {
	return &Coordinator{
		current:     map[string]model.TransitCycle{cycle.ChamberID: cycle},
		generations: map[string]uint64{cycle.ChamberID: cycle.Generation},
	}
}

func TestRequireActiveAcceptsCurrentNonTerminalTransit(t *testing.T) {
	cycle, err := model.NewTransitCycle("chamber-1", uuid.New(), model.DirectionUpstream, 1, time.Now())
	if err != nil {
		t.Fatalf("cycle: %v", err)
	}
	coordinator := guardCoordinator(cycle)
	current, err := coordinator.RequireActive(cycle)
	if err != nil {
		t.Fatalf("RequireActive rejected a live transit: %v", err)
	}
	if !current.IdentityMatches(cycle.ID, cycle.Generation) {
		t.Fatal("returned cycle identity mismatch")
	}
}

func TestRequireActiveRejectsTerminalTransit(t *testing.T) {
	for _, stage := range []model.TransitStage{model.StageCancelled, model.StageEmergency, model.StageCompleted} {
		cycle, err := model.NewTransitCycle("chamber-1", uuid.New(), model.DirectionUpstream, 1, time.Now())
		if err != nil {
			t.Fatalf("cycle: %v", err)
		}
		coordinator := guardCoordinator(cycle)
		// Place the current transit directly into each terminal stage.
		cycle.Stage = stage
		coordinator.current["chamber-1"] = cycle
		if _, err := coordinator.RequireActive(cycle); err == nil {
			t.Fatalf("expected rejection for terminal stage %s", stage)
		}
	}
}

func TestRequireActiveRejectsStaleGeneration(t *testing.T) {
	cycle, err := model.NewTransitCycle("chamber-1", uuid.New(), model.DirectionUpstream, 1, time.Now())
	if err != nil {
		t.Fatalf("cycle: %v", err)
	}
	// A newer generation supersedes the cycle captured by the ramp.
	newer := cycle
	newer.Generation = 2
	newer.Stage = model.StageLeveling
	coordinator := guardCoordinator(cycle)
	coordinator.current["chamber-1"] = newer

	_, err = coordinator.RequireActive(cycle)
	if err == nil {
		t.Fatal("expected rejection when a newer transit generation is current")
	}
	if !strings.Contains(err.Error(), "generation") {
		t.Fatalf("error should mention generation change, got: %v", err)
	}
}

func TestRequireActiveRejectsMissingChamber(t *testing.T) {
	cycle, err := model.NewTransitCycle("chamber-9", uuid.New(), model.DirectionUpstream, 1, time.Now())
	if err != nil {
		t.Fatalf("cycle: %v", err)
	}
	coordinator := guardCoordinator(cycle)
	delete(coordinator.current, "chamber-9")
	if _, err := coordinator.RequireActive(cycle); err == nil {
		t.Fatal("expected rejection with no current transit")
	}
}
