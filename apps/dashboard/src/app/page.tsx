import Link from "next/link";
import {AuthMe, Call, VoiceAgent} from "@/lib/api";
import {serverFetchJson} from "@/lib/serverApi";

export default async function Home() {
  const [me, agents, calls] = await Promise.all([
    serverFetchJson<AuthMe>("/auth/me"),
    serverFetchJson<VoiceAgent[]>("/agents"),
    serverFetchJson<Call[]>("/calls"),
  ]);
  const activeCalls = calls.filter((call) => !call.ended_at && call.status !== "CALL_COMPLETED");

  return (
    <div className="stack">
      <section className="hero compact">
        <div className="eyebrow">{me.organization.name}</div>
        <h1>Voice agent operations workspace</h1>
        <p>
          Manage voice agents, test live calls, inspect per-agent call history,
          and track billing/observability from one organization-scoped workspace.
        </p>
        <div className="actions">
          <Link className="button primary" href="/agents/new">Create agent</Link>
          <Link className="button" href="/agents">View agents</Link>
        </div>
      </section>
      <section className="grid four">
        <div className="card"><h3>Agents</h3><div className="metric">{agents.length}</div></div>
        <div className="card"><h3>Recent calls</h3><div className="metric">{calls.length}</div></div>
        <div className="card"><h3>Active calls</h3><div className="metric">{activeCalls.length}</div></div>
        <div className="card"><h3>Signed in as</h3><div className="metric small-text">{me.user.email}</div></div>
      </section>
      <section className="card">
        <div className="row">
          <h2>Recent voice agents</h2>
          <Link className="button small" href="/agents">Manage</Link>
        </div>
        <div className="grid three">
          {agents.slice(0, 3).map((agent) => (
            <Link className="agent-card" href={`/agents/${agent.id}`} key={agent.id}>
              <div className="status idle">{agent.status}</div>
              <h3>{agent.name}</h3>
              <p>{agent.description || "No description yet."}</p>
              <div className="mono muted">{agent.llm_model} / {agent.tts_voice}</div>
            </Link>
          ))}
        </div>
      </section>
    </div>
  );
}
