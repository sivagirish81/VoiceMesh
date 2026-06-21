export const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";
export const SERVER_API_URL = process.env.API_INTERNAL_URL ?? API_URL;
export const WS_URL = process.env.NEXT_PUBLIC_WS_URL ?? "ws://localhost:8000";

export type Call = {
  call_id: string;
  organization_id?: string;
  agent_id?: string;
  agent_name?: string;
  agent_snapshot?: Record<string, unknown>;
  status: string;
  current_stage: string;
  corked: boolean;
  cork_reason?: string;
  selected_stt_provider: string;
  selected_llm_provider: string;
  selected_tts_provider: string;
  selected_stt_model?: string;
  selected_llm_model?: string;
  selected_tts_model?: string;
  error?: string;
  created_at: string;
  started_at?: string;
  ended_at?: string | null;
};

export type AuthMe = {
  user: {id: string; email: string; name: string};
  organization: {id: string; name: string};
  role: string;
};

export type VoiceAgent = {
  id: string;
  organization_id: string;
  name: string;
  description: string;
  status: "active" | "paused";
  system_prompt: string;
  context_prompt: string;
  first_message: string;
  stt_provider: string;
  stt_model: string;
  llm_provider: string;
  llm_model: string;
  tts_provider: string;
  tts_model: string;
  tts_voice: string;
  recent_call_count?: number;
  last_call_at?: string | null;
};

export type PipelineEvent = {
  event_id: string;
  call_id: string;
  turn_id: string;
  event_type: string;
  stage: string;
  timestamp?: string;
  created_at?: string;
  sequence_number: number;
  payload: Record<string, unknown>;
  trace_id?: string;
};

export type BillingCall = {
  call_id: string;
  call_duration_seconds: string | number;
  provider_cost_usd: string | number;
  platform_fee_usd: string | number;
  total_cost_usd: string | number;
  platform_cost_cents?: string | number;
  stt_cost_cents?: string | number;
  llm_cost_cents?: string | number;
  tts_cost_cents?: string | number;
  telephony_cost_cents?: string | number;
  total_cost_cents?: string | number;
  currency: string;
  status: string;
  call_status?: string;
  pricing_version: string;
  updated_at: string;
};

export async function fetchJson<T>(path: string, options?: RequestInit): Promise<T> {
  const baseUrl = typeof window === "undefined" ? API_URL : "/api/backend";
  const response = await fetch(`${baseUrl}${path}`, {
    ...options,
    headers: {"Content-Type": "application/json", ...(options?.headers ?? {})},
    cache: "no-store",
    credentials: "include",
  });
  if (!response.ok) throw new Error(`${response.status} ${await response.text()}`);
  return response.json() as Promise<T>;
}
