import Link from "next/link";
import {CallConsole} from "@/app/demo/page";
import {Call, VoiceAgent} from "@/lib/api";
import {serverFetchJson} from "@/lib/serverApi";

export default async function AgentDetailPage({
  params,
}: {
  params: Promise<{agent_id: string}>;
}) {
  const {agent_id} = await params;
  const [agent, calls] = await Promise.all([
    serverFetchJson<VoiceAgent>(`/agents/${agent_id}`),
    serverFetchJson<Call[]>(`/agents/${agent_id}/calls`),
  ]);
  return (
    <div className="stack">
      <section className="row" style={{alignItems: "flex-end"}}>
        <div>
          <div className="eyebrow">Voice agent</div>
          <h1>{agent.name}</h1>
          <p className="muted">{agent.description || "No description yet."}</p>
        </div>
        <Link className="button" href={`/agents/${agent.id}/edit`}>Edit agent</Link>
      </section>
      <div className="grid three">
        <div className="card"><h3>Status</h3><div className="status idle">{agent.status}</div></div>
        <div className="card"><h3>LLM</h3><div className="mono muted">{agent.llm_provider} / {agent.llm_model}</div></div>
        <div className="card"><h3>Voice</h3><div className="mono muted">{agent.tts_model} / {agent.tts_voice}</div></div>
      </div>
      <section className="card">
        <h2>Agent context</h2>
        <p className="muted whitespace">{agent.context_prompt || "No extra context configured."}</p>
      </section>
      <CallConsole
        agentId={agent.id}
        title={`Test ${agent.name}`}
        eyebrow="Selected agent -> real providers -> browser audio"
      />
      <section className="card">
        <div className="row">
          <h2>Recent calls</h2>
          <Link className="button small" href="/calls">All calls</Link>
        </div>
        <div className="table">
          {calls.map((call) => (
            <Link className="table-row" href={`/calls/${call.call_id}`} key={call.call_id}>
              <span className="mono">{call.call_id}</span>
              <span>{call.status}</span>
              <span>{call.current_stage}</span>
              <span>{call.created_at ? new Date(call.created_at).toLocaleString() : ""}</span>
            </Link>
          ))}
          {calls.length === 0 && <p className="muted">No calls for this agent yet.</p>}
        </div>
      </section>
    </div>
  );
}
