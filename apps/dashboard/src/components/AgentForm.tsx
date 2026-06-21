"use client";

import {FormEvent, useState} from "react";
import {useRouter} from "next/navigation";
import {fetchJson, VoiceAgent} from "@/lib/api";

const DEFAULT_SYSTEM_PROMPT =
  "You are a concise, helpful voice agent. Speak naturally and ask one question at a time.";

export function AgentForm({agent}: {agent?: VoiceAgent}) {
  const router = useRouter();
  const [form, setForm] = useState({
    name: agent?.name ?? "",
    description: agent?.description ?? "",
    status: agent?.status ?? "active",
    system_prompt: agent?.system_prompt ?? DEFAULT_SYSTEM_PROMPT,
    context_prompt: agent?.context_prompt ?? "",
    first_message: agent?.first_message ?? "",
    stt_model: agent?.stt_model ?? "gpt-realtime-whisper",
    llm_model: agent?.llm_model ?? "gpt-4.1-mini",
    tts_model: agent?.tts_model ?? "gpt-4o-mini-tts",
    tts_voice: agent?.tts_voice ?? "alloy",
  });
  const [error, setError] = useState("");

  function update(field: keyof typeof form, value: string) {
    setForm((current) => ({...current, [field]: value}));
  }

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError("");
    const payload = {
      ...form,
      stt_provider: "openai",
      llm_provider: "openai",
      tts_provider: "openai",
    };
    try {
      const saved = agent
        ? await fetchJson<VoiceAgent>(`/agents/${agent.id}`, {
            method: "PATCH",
            body: JSON.stringify(payload),
          })
        : await fetchJson<VoiceAgent>("/agents", {
            method: "POST",
            body: JSON.stringify(payload),
          });
      router.push(`/agents/${saved.id}`);
      router.refresh();
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Unable to save agent");
    }
  }

  return (
    <form className="card form-grid" onSubmit={onSubmit}>
      <label className="field">
        <span>Name</span>
        <input required value={form.name} onChange={(event) => update("name", event.target.value)} />
      </label>
      <label className="field">
        <span>Status</span>
        <select value={form.status} onChange={(event) => update("status", event.target.value)}>
          <option value="active">Active</option>
          <option value="paused">Paused</option>
        </select>
      </label>
      <label className="field wide">
        <span>Description</span>
        <input value={form.description} onChange={(event) => update("description", event.target.value)} />
      </label>
      <label className="field wide">
        <span>System prompt</span>
        <textarea rows={5} value={form.system_prompt} onChange={(event) => update("system_prompt", event.target.value)} />
      </label>
      <label className="field wide">
        <span>Agent context</span>
        <textarea rows={8} value={form.context_prompt} onChange={(event) => update("context_prompt", event.target.value)} />
      </label>
      <label className="field wide">
        <span>First message</span>
        <input value={form.first_message} onChange={(event) => update("first_message", event.target.value)} />
      </label>
      <label className="field">
        <span>STT model</span>
        <input value={form.stt_model} onChange={(event) => update("stt_model", event.target.value)} />
      </label>
      <label className="field">
        <span>LLM model</span>
        <input value={form.llm_model} onChange={(event) => update("llm_model", event.target.value)} />
      </label>
      <label className="field">
        <span>TTS model</span>
        <input value={form.tts_model} onChange={(event) => update("tts_model", event.target.value)} />
      </label>
      <label className="field">
        <span>TTS voice</span>
        <input value={form.tts_voice} onChange={(event) => update("tts_voice", event.target.value)} />
      </label>
      {error && <div className="error wide">{error}</div>}
      <div className="actions wide">
        <button className="button primary" type="submit">Save agent</button>
      </div>
    </form>
  );
}
