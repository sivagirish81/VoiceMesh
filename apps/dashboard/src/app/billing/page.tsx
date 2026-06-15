import Link from "next/link";
import {BillingCall, SERVER_API_URL} from "@/lib/api";

type BillingSummary = {
  totals: {
    calls?: number;
    duration_seconds?: string | number;
    provider_cost_usd?: string | number;
    platform_fee_usd?: string | number;
    total_cost_usd?: string | number;
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

async function load<T>(path: string, fallback: T): Promise<T> {
  try {
    const response = await fetch(`${SERVER_API_URL}${path}`, {cache: "no-store"});
    return response.ok ? response.json() : fallback;
  } catch {
    return fallback;
  }
}

function usd(value: string | number | undefined): string {
  return `$${Number(value ?? 0).toFixed(6)}`;
}

export default async function BillingPage() {
  const [summary, calls] = await Promise.all([
    load<BillingSummary>("/billing/summary", {totals: {}, usage: []}),
    load<BillingCall[]>("/billing/calls", []),
  ]);
  const durationMinutes = Number(summary.totals.duration_seconds ?? 0) / 60;

  return (
    <div className="stack">
      <section>
        <div className="eyebrow">Kafka usage events → event worker → Postgres ledger</div>
        <h1 style={{fontSize: 46}}>Billing</h1>
        <p>Provider cost is derived from metered usage. The platform fee is the configurable VoiceMesh lab rate per connected call minute.</p>
      </section>

      <div className="grid three">
        <div className="card"><h3>Total billed</h3><div className="metric">{usd(summary.totals.total_cost_usd)}</div></div>
        <div className="card"><h3>Provider cost</h3><div className="metric">{usd(summary.totals.provider_cost_usd)}</div></div>
        <div className="card"><h3>Platform fee</h3><div className="metric">{usd(summary.totals.platform_fee_usd)}</div></div>
        <div className="card"><h3>Metered calls</h3><div className="metric">{Number(summary.totals.calls ?? 0)}</div></div>
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
          <thead><tr><th>Call</th><th>Duration</th><th>Provider</th><th>Platform</th><th>Total</th><th>Status</th></tr></thead>
          <tbody>
            {calls.map((call) => (
              <tr key={call.call_id}>
                <td><Link className="event-name mono" href={`/calls/${call.call_id}`}>{call.call_id.slice(0, 18)}</Link></td>
                <td>{(Number(call.call_duration_seconds) / 60).toFixed(2)} min</td>
                <td>{usd(call.provider_cost_usd)}</td>
                <td>{usd(call.platform_fee_usd)}</td>
                <td>{usd(call.total_cost_usd)}</td>
                <td><span className="status">{call.status}</span></td>
              </tr>
            ))}
            {calls.length === 0 && <tr><td colSpan={6} className="muted">No billing records yet.</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  );
}
