const rampUI = (() => {
  const { api, shortID, setMessage } = window.LockFlow;
  const chamberSelect = document.querySelector("#ramp-chamber");
  const targetsInput = document.querySelector("#ramp-targets");
  const message = document.querySelector("#ramp-message");
  const startButton = document.querySelector("#ramp-start");
  const pauseButton = document.querySelector("#ramp-pause");
  const resumeButton = document.querySelector("#ramp-resume");
  const cancelButton = document.querySelector("#ramp-cancel");
  const reviewerInput = document.querySelector("#ramp-reviewer");
  const reviewCheck = document.querySelector("#ramp-review");

  // Token issued at the latest pause; required together with a fresh safety
  // review so resumption cannot reuse a stale confirmation.
  let resumeToken = "";

  const base = () => `/api/chambers/${encodeURIComponent(chamberSelect.value)}/valves/ramp`;

  const stateLabel = { pending: "Pending", issued: "Issued", completed: "Completed" };

  function render(view) {
    document.querySelector("#ramp-phase").textContent = view.phase || "-";
    document.querySelector("#ramp-progress").textContent = `${view.completed_count} / ${view.remaining_count}`;
    document.querySelector("#ramp-next").textContent =
      view.next_index !== undefined && view.next_index !== null
        ? `#${view.next_index + 1} · ${shortID(view.cycle_id)}`
        : `done · ${shortID(view.cycle_id)}`;
    document.querySelector("#ramp-steps").innerHTML = view.steps.map((step) =>
      `<tr><td>${step.index + 1}</td><td>${step.target}</td><td>${stateLabel[step.state] || step.state}</td><td>${shortID(step.command_id)}</td></tr>`
    ).join("");

    const active = view.phase === "running" || view.phase === "paused";
    startButton.disabled = active;
    pauseButton.disabled = view.phase !== "running";
    cancelButton.disabled = !active;
    if (view.phase === "paused" && view.resume_token) {
      resumeToken = view.resume_token.token;
    }
    syncResume();
    if (view.reason && !active) {
      setMessage(message, `${view.phase}: ${view.reason}`);
    }
  }

  function syncResume() {
    const phaseText = document.querySelector("#ramp-phase").textContent;
    resumeButton.disabled = !(phaseText === "paused" && reviewerInput.value.trim() && reviewCheck.checked && resumeToken);
  }
  reviewerInput.addEventListener("input", syncResume);
  reviewCheck.addEventListener("change", syncResume);

  async function refresh() {
    try {
      const view = await api(base());
      render(view);
    } catch (error) {
      // 404 simply means this chamber has no ramp yet.
      resumeToken = "";
      document.querySelector("#ramp-steps").innerHTML =
        '<tr><td colspan="4" class="muted">No ramp for this chamber</td></tr>';
      document.querySelector("#ramp-phase").textContent = "-";
      document.querySelector("#ramp-progress").textContent = "-";
      document.querySelector("#ramp-next").textContent = "-";
      startButton.disabled = false;
      pauseButton.disabled = true;
      resumeButton.disabled = true;
      cancelButton.disabled = true;
    }
  }

  startButton.addEventListener("click", async () => {
    const targets = targetsInput.value.split(",").map((value) => Number(value.trim())).filter((value) => !Number.isNaN(value));
    if (targets.length === 0 || targets.length > 12) {
      setMessage(message, "Enter between 1 and 12 numeric targets.");
      return;
    }
    startButton.disabled = true;
    try {
      const view = await api(base(), { method: "POST", body: JSON.stringify({ targets }) });
      setMessage(message, `Ramp ${shortID(view.id)} started.`);
      await refresh();
    } catch (error) {
      setMessage(message, error.message);
      startButton.disabled = false;
    }
  });

  pauseButton.addEventListener("click", async () => {
    pauseButton.disabled = true;
    try {
      const view = await api(`${base()}/pause`, { method: "POST" });
      resumeToken = view.resume_token ? view.resume_token.token : "";
      render(view);
      setMessage(message, "Paused. Complete the safety review, then resume from the next pending step.");
    } catch (error) {
      setMessage(message, error.message);
    }
  });

  resumeButton.addEventListener("click", async () => {
    if (!resumeToken) {
      setMessage(message, "Refresh the paused ramp to obtain a current resume token.");
      return;
    }
    resumeButton.disabled = true;
    try {
      const view = await api(`${base()}/resume`, {
        method: "POST",
        body: JSON.stringify({
          resume_token: resumeToken,
          safety_review: reviewCheck.checked,
          reviewer: reviewerInput.value.trim(),
        }),
      });
      reviewCheck.checked = false;
      render(view);
      setMessage(message, "Resumed from next pending step.");
    } catch (error) {
      // A cancelled/emergency/superseded transit invalidates the token; the
      // next poll shows the terminal phase.
      setMessage(message, error.message);
      resumeToken = "";
      await refresh();
    }
  });

  cancelButton.addEventListener("click", async () => {
    cancelButton.disabled = true;
    try {
      const view = await api(`${base()}/cancel`, { method: "POST" });
      render(view);
      setMessage(message, "Ramp cancelled; issued openings were not re-sent.");
    } catch (error) {
      setMessage(message, error.message);
    }
  });

  chamberSelect.addEventListener("change", () => {
    setMessage(message, "");
    resumeToken = "";
    reviewCheck.checked = false;
    refresh();
  });

  refresh();
  setInterval(refresh, 2000);
  return { refresh };
})();
