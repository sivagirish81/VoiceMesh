export const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";
export const SERVER_API_URL = process.env.API_INTERNAL_URL ?? API_URL;
export const WS_URL = process.env.NEXT_PUBLIC_WS_URL ?? "ws://localhost:8000";

export type Call = {
  call_id: string;
  status: string;
  current_stage: string;
  corked: boolean;
  cork_reason?: string;
  selected_stt_provider: string;
  selected_llm_provider: string;
  selected_tts_provider: string;
  error?: string;
  created_at: string;
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
  const response = await fetch(`${API_URL}${path}`, {
    ...options,
    headers: {"Content-Type": "application/json", ...(options?.headers ?? {})},
    cache: "no-store",
  });
  if (!response.ok) throw new Error(`${response.status} ${await response.text()}`);
  return response.json() as Promise<T>;
}
