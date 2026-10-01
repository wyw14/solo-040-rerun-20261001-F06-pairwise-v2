package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type rampInput struct {
	Targets []float64 `json:"targets"`
}

type rampResumeInput struct {
	ResumeToken  string `json:"resume_token"`
	SafetyReview bool   `json:"safety_review"`
	Reviewer     string `json:"reviewer"`
	Note         string `json:"note"`
}

// valveRampStartHandler starts a background valve opening ramp. The call
// returns immediately (202) with the full step plan; progress is observed via
// GET and the pause/resume/cancel actions.
func valveRampStartHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input rampInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if len(input.Targets) == 0 || len(input.Targets) > 12 {
			writeError(w, http.StatusBadRequest, errors.New("targets must contain 1 to 12 steps"))
			return
		}
		chamberID := chi.URLParam(r, "chamberID")
		cycleValue, ok := runtime.Cycles.Current(chamberID)
		if !ok {
			writeError(w, http.StatusConflict, errors.New("active transit not found"))
			return
		}
		if cycleValue.Stage.Terminal() {
			writeError(w, http.StatusConflict, errors.New("transit is "+string(cycleValue.Stage)+"; ramp cannot start"))
			return
		}
		if existing, ok := runtime.Hydraulics.RampView(chamberID); ok && !existing.Phase.Terminal() {
			writeError(w, http.StatusConflict, errors.New("a ramp is already active for this chamber"))
			return
		}
		view, err := runtime.Hydraulics.StartRamp(r.Context(), cycleValue, input.Targets)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusAccepted, view)
	}
}

func valveRampViewHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		view, ok := runtime.Hydraulics.RampView(chamberID)
		if !ok {
			writeError(w, http.StatusNotFound, errors.New("no ramp for this chamber"))
			return
		}
		writeJSON(w, http.StatusOK, view)
	}
}

func valveRampPauseHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		view, err := runtime.Hydraulics.PauseRamp(r.Context(), chamberID)
		if err != nil {
			writeRampError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, view)
	}
}

// valveRampResumeHandler continues a paused ramp from its next pending step.
// It requires a fresh safety review and the resume token returned at pause; a
// cancelled/emergency/superseded transit is rejected so an opening is never
// re-issued against a stale generation.
func valveRampResumeHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input rampResumeInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if !input.SafetyReview {
			writeError(w, http.StatusBadRequest, errors.New("resume requires a confirmed safety review"))
			return
		}
		if strings.TrimSpace(input.Reviewer) == "" {
			writeError(w, http.StatusBadRequest, errors.New("resume requires the reviewing operator name"))
			return
		}
		token, err := uuid.Parse(input.ResumeToken)
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("invalid resume token"))
			return
		}
		chamberID := chi.URLParam(r, "chamberID")
		view, err := runtime.Hydraulics.ResumeRamp(r.Context(), chamberID, token)
		if err != nil {
			writeRampError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, view)
	}
}

func valveRampCancelHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		view, err := runtime.Hydraulics.CancelRamp(r.Context(), chamberID, "operator cancelled the ramp")
		if err != nil {
			writeRampError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, view)
	}
}

// writeRampError maps a missing ramp to 404 and state/generation conflicts to
// 409; those are the two failure classes for ramp lifecycle actions.
func writeRampError(w http.ResponseWriter, err error) {
	if strings.Contains(err.Error(), "no ramp") {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeError(w, http.StatusConflict, err)
}
