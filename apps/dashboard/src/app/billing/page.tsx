import Link from "next/link";
import {BillingCall} from "@/lib/api";
import {serverFetchJson} from "@/lib/serverApi";

type BillingSummary = {
  totals: {
    calls?: number;
    duration_seconds?: string | number;
    provider_cost_usd?: string | number;
    platform_fee_usd?: string | number;
    total_cost_usd?: string | number;
    finalized_calls?: string | number;
    finalized_duration_seconds?: string | number;
    platform_cost_cents?: string | number;
    stt_cost_cents?: string | number;
    llm_cost_cents?: string | number;
    tts_cost_cents?: string | number;
    telephony_cost_cents?: string | number;
    total_cost_cents?: string | number;
  };
  usage: Array<{
    stage: string;
    provider: string;
    model: string;
    usage_type: string;
    unit: string;
    quantity: string | number;
    cost_usd: string | number;
    has_estimates: boolean;
  }>;
};

function usd(value: string | number | undefined): string {
  return `$${Number(value ?? 0).toFixed(6)}`;
}

function centsUsd(value: string | number | undefined): string {
  return `$${(Number(value ?? 0) / 100).toFixed(2)}`;
}

export default async function BillingPage() {
  const [summary, calls] = await Promise.all([
    serverFetchJson<BillingSummary>("/billing/summary"),
    serverFetchJson<BillingCall[]>("/billing/calls"),
  ]);
  const durationSeconds = Number(
    summary.totals.finalized_duration_seconds ?? summary.totals.duration_seconds ?? 0
  );
  const durationMinutes = durationSeconds / 60;
  const providerCostCents =
    Number(summary.totals.stt_cost_cents ?? 0) +
    Number(summary.totals.llm_cost_cents ?? 0) +
    Number(summary.totals.tts_cost_cents ?? 0) +
    Number(summary.totals.telephony_cost_cents ?? 0);
  const totalBilled =
    summary.totals.total_cost_cents !== undefined
      ? centsUsd(summary.totals.total_cost_cents)
      : usd(summary.totals.total_cost_usd);
  const providerBilled =
    providerCostCents > 0 ? centsUsd(providerCostCents) : usd(summary.totals.provider_cost_usd);
  const platformBilled =
    summary.totals.platform_cost_cents !== undefined
      ? centsUsd(summary.totals.platform_cost_cents)
      : usd(summary.totals.platform_fee_usd);

  return (
    <div className="stack">
      <section>
        <div className="eyebrow">Kafka usage events → event worker → Postgres ledger</div>
        <h1 style={{fontSize: 46}}>Billing</h1>
        <p>Provider cost is derived from metered usage. The platform fee is the configurable VoiceMesh lab rate per connected call minute.</p>
      </section>

      <div className="grid three">
        <div className="card"><h3>Total billed</h3><div className="metric">{totalBilled}</div></div>
        <div className="card"><h3>Provider cost</h3><div className="metric">{providerBilled}</div></div>
        <div className="card"><h3>Platform fee</h3><div className="metric">{platformBilled}</div></div>
        <div className="card"><h3>Finalized calls</h3><div className="metric">{Number(summary.totals.finalized_calls ?? summary.totals.calls ?? 0)}</div></div>
        <div className="card"><h3>Connected minutes</h3><div className="metric">{durationMinutes.toFixed(2)}</div></div>
        <div className="card"><h3>Cost confidence</h3><div className="metric">{summary.usage.some((row) => row.has_estimates) ? "Mixed" : "Exact"}</div><p>TTS token counts are estimated from text and PCM duration because the speech endpoint does not return token usage.</p></div>
      </div>

      <div className="card">
        <h2>Usage by provider stage</h2>
        <table className="table">
          <thead><tr><th>Stage</th><th>Model</th><th>Usage</th><th>Quantity</th><th>Cost</th><th>Quality</th></tr></thead>
          <tbody>
            {summary.usage.map((row) => (
              <tr key={`${row.stage}-${row.model}-${row.usage_type}`}>
                <td>{row.stage}</td>
                <td className="mono">{row.provider}/{row.model}</td>
                <td>{row.usage_type}</td>
                <td>{Number(row.quantity).toFixed(3)} {row.unit}</td>
                <td>{usd(row.cost_usd)}</td>
                <td>{row.has_estimates ? "estimated" : "metered"}</td>
              </tr>
            ))}
            {summary.usage.length === 0 && <tr><td colSpan={6} className="muted">Complete a call to create usage records.</td></tr>}
          </tbody>
        </table>
      </div>

      <div className="card">
        <h2>Per-call ledger</h2>
        <table className="table">
          <thead><tr><th>Call</th><th>Duration</th><th>Platform</th><th>STT</th><th>LLM</th><th>TTS</th><th>Total</th><th>Status</th></tr></thead>
          <tbody>
            {calls.map((call) => (
              <tr key={call.call_id}>
                <td><Link className="event-name mono" href={`/calls/${call.call_id}`}>{call.call_id.slice(0, 18)}</Link></td>
                <td>{(Number(call.call_duration_seconds) / 60).toFixed(2)} min</td>
                <td>{call.platform_cost_cents !== null && call.platform_cost_cents !== undefined ? centsUsd(call.platform_cost_cents) : usd(call.platform_fee_usd)}</td>
                <td>{call.stt_cost_cents !== null && call.stt_cost_cents !== undefined ? centsUsd(call.stt_cost_cents) : "n/a"}</td>
                <td>{call.llm_cost_cents !== null && call.llm_cost_cents !== undefined ? centsUsd(call.llm_cost_cents) : "n/a"}</td>
                <td>{call.tts_cost_cents !== null && call.tts_cost_cents !== undefined ? centsUsd(call.tts_cost_cents) : "n/a"}</td>
                <td>{call.total_cost_cents !== null && call.total_cost_cents !== undefined ? centsUsd(call.total_cost_cents) : usd(call.total_cost_usd)}</td>
                <td><span className="status">{call.status}</span></td>
              </tr>
            ))}
            {calls.length === 0 && <tr><td colSpan={8} className="muted">No billing records yet.</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  );
}
