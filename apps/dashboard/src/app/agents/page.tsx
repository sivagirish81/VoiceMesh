import Link from "next/link";
import {VoiceAgent} from "@/lib/api";
import {serverFetchJson} from "@/lib/serverApi";

export default async function AgentsPage() {
  const agents = await serverFetchJson<VoiceAgent[]>("/agents");
  return (
    <div className="stack">
      <section className="row" style={{alignItems: "flex-end"}}>
        <div>
          <div className="eyebrow">Workspace agents</div>
          <h1>Voice agents</h1>
          <p className="muted">Configure agent context, provider models, voice, and test calls.</p>
        </div>
        <Link className="button primary" href="/agents/new">New agent</Link>
      </section>
      <div className="grid three">
        {agents.map((agent) => (
          <Link className="agent-card" href={`/agents/${agent.id}`} key={agent.id}>
            <div className="status idle">{agent.status}</div>
            <h3>{agent.name}</h3>
            <p>{agent.description || "No description yet."}</p>
            <div className="mono muted">
              STT {agent.stt_model}<br />
              LLM {agent.llm_model}<br />
              TTS {agent.tts_model} / {agent.tts_voice}
            </div>
            <div className="metric small-text">{agent.recent_call_count ?? 0} calls</div>
          </Link>
        ))}
      </div>
    </div>
  );
}
