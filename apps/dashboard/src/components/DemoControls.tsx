"use client";

import {useEffect, useState} from "react";
import {fetchJson} from "@/lib/api";

type FailureState = {
  enabled: boolean;
  stt_delay_ms: number;
  llm_delay_ms: number;
  tts_delay_ms: number;
  provider_failure: boolean;
  postgres_failure: boolean;
  stage_timeout: boolean;
};

const empty: FailureState = {
  enabled: false,
  stt_delay_ms: 0,
  llm_delay_ms: 0,
  tts_delay_ms: 0,
  provider_failure: false,
  postgres_failure: false,
  stage_timeout: false,
};

export function DemoControls({callId}: {callId: string}) {
  const [state, setState] = useState<FailureState>(empty);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    fetchJson<FailureState>("/demo/failure-injection").then(setState).catch(() => undefined);
  }, []);

  async function apply(patch: Partial<FailureState>) {
    setBusy(true);
    try {
      const next = await fetchJson<FailureState>("/demo/failure-injection", {
        method: "POST",
        body: JSON.stringify({...patch, enabled: patch.enabled ?? true}),
      });
      setState(next);
    } finally {
      setBusy(false);
    }
  }

  async function replay() {
    if (!callId) return;
    setBusy(true);
    try {
      await fetchJson(`/demo/replay-duplicate-events/${callId}`, {method: "POST"});
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="card">
      <div className="row">
        <h2>Failure injection</h2>
        <span className={`status ${state.enabled ? "corked" : "idle"}`}>
          {state.enabled ? "armed" : "off"}
        </span>
      </div>
      <div className="controls">
        <div className="control">
          <h3>TTS delay</h3>
          <input
            aria-label="TTS delay"
            type="range"
            min="0"
            max="3000"
            step="250"
            value={state.tts_delay_ms}
            onChange={(event) => setState({...state, tts_delay_ms: Number(event.target.value)})}
            onMouseUp={() => apply({tts_delay_ms: state.tts_delay_ms})}
            onTouchEnd={() => apply({tts_delay_ms: state.tts_delay_ms})}
          />
          <span className="mono">{state.tts_delay_ms} ms / chunk</span>
        </div>
        <div className="control">
          <h3>Provider failure</h3>
          <button className="button danger" disabled={busy} onClick={() => apply({provider_failure: !state.provider_failure})}>
            {state.provider_failure ? "Disarm failure" : "Fail next provider"}
          </button>
        </div>
        <div className="control">
          <h3>Duplicate replay</h3>
          <button className="button" disabled={busy || !callId} onClick={replay}>
            Replay latest event
          </button>
        </div>
      </div>
      <div className="actions">
        <button className="button" disabled={busy} onClick={() => apply({...empty})}>Reset injection</button>
        <button className="button danger" disabled={busy} onClick={() => apply({postgres_failure: !state.postgres_failure})}>
          {state.postgres_failure ? "Restore DB writes" : "Fail DB writes"}
        </button>
      </div>
    </div>
  );
}

