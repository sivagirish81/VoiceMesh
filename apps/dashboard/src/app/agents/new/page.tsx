import {AgentForm} from "@/components/AgentForm";

export default function NewAgentPage() {
  return (
    <div className="stack">
      <section>
        <div className="eyebrow">Create voice agent</div>
        <h1>New agent</h1>
        <p className="muted">Start with text context and provider choices. Tools and knowledge bases can come later.</p>
      </section>
      <AgentForm />
    </div>
  );
}
