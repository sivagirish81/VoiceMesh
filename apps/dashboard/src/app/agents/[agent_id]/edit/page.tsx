import {AgentForm} from "@/components/AgentForm";
import {VoiceAgent} from "@/lib/api";
import {serverFetchJson} from "@/lib/serverApi";

export default async function EditAgentPage({
  params,
}: {
  params: Promise<{agent_id: string}>;
}) {
  const {agent_id} = await params;
  const agent = await serverFetchJson<VoiceAgent>(`/agents/${agent_id}`);
  return (
    <div className="stack">
      <section>
        <div className="eyebrow">Edit voice agent</div>
        <h1>{agent.name}</h1>
        <p className="muted">Changes apply to future calls. Existing calls keep their agent snapshot.</p>
      </section>
      <AgentForm agent={agent} />
    </div>
  );
}
