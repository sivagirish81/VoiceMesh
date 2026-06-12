import Link from "next/link";
import {API_URL, Call} from "@/lib/api";

async function getCalls(): Promise<Call[]> {
  try {
    const response = await fetch(`${API_URL}/calls`, {cache: "no-store"});
    return response.ok ? response.json() : [];
  } catch {
    return [];
  }
}

export default async function CallsPage() {
  const calls = await getCalls();
  return (
    <div className="stack">
      <section>
        <div className="eyebrow">Durable session inventory</div>
        <h1 style={{fontSize: 46}}>Calls</h1>
      </section>
      <div className="card">
        <table className="table">
          <thead><tr><th>Call</th><th>Status</th><th>Stage</th><th>Providers</th><th>Created</th></tr></thead>
          <tbody>
            {calls.map((call) => (
              <tr key={call.call_id}>
                <td><Link className="event-name mono" href={`/calls/${call.call_id}`}>{call.call_id.slice(0, 12)}</Link></td>
                <td><span className={`status ${call.status.includes("FAILED") ? "failed" : ""}`}>{call.status}</span></td>
                <td>{call.current_stage}{call.corked ? " · corked" : ""}</td>
                <td className="mono">{call.selected_stt_provider} / {call.selected_llm_provider} / {call.selected_tts_provider}</td>
                <td>{new Date(call.created_at).toLocaleString()}</td>
              </tr>
            ))}
            {calls.length === 0 && <tr><td colSpan={5} className="muted">No persisted calls yet.</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  );
}

