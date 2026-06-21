import Link from "next/link";
import {Call} from "@/lib/api";
import {serverFetchJson} from "@/lib/serverApi";

export default async function CallsPage() {
  const calls = await serverFetchJson<Call[]>("/calls");
  return (
    <div className="stack">
      <section>
        <div className="eyebrow">Organization call inventory</div>
        <h1 style={{fontSize: 46}}>Calls</h1>
      </section>
      <div className="card">
        <table className="table">
          <thead><tr><th>Call</th><th>Agent</th><th>Status</th><th>Stage</th><th>Models</th><th>Created</th></tr></thead>
          <tbody>
            {calls.map((call) => (
              <tr key={call.call_id}>
                <td><Link className="event-name mono" href={`/calls/${call.call_id}`}>{call.call_id.slice(0, 12)}</Link></td>
                <td>{call.agent_name ?? "Default agent"}</td>
                <td><span className={`status ${call.status.includes("FAILED") ? "failed" : ""}`}>{call.status}</span></td>
                <td>{call.current_stage}{call.corked ? " · corked" : ""}</td>
                <td className="mono">{call.selected_llm_model ?? call.selected_llm_provider} / {call.selected_tts_model ?? call.selected_tts_provider}</td>
                <td>{new Date(call.created_at).toLocaleString()}</td>
              </tr>
            ))}
            {calls.length === 0 && <tr><td colSpan={6} className="muted">No persisted calls yet.</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  );
}
