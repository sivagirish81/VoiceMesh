import {CallTimeline} from "@/components/CallTimeline";
import {PipelineState} from "@/components/PipelineState";
import {Call, PipelineEvent} from "@/lib/api";
import {serverFetchJson} from "@/lib/serverApi";

export default async function CallDetail({params}: {params: Promise<{call_id: string}>}) {
  const {call_id} = await params;
  const [call, events] = await Promise.all([
    serverFetchJson<Call>(`/calls/${call_id}`),
    serverFetchJson<PipelineEvent[]>(`/calls/${call_id}/events`),
  ]);
  if (!call) return <div className="card">Call not found or API unavailable.</div>;
  const eventList = events ?? [];
  const duplicates = eventList.filter((event) => event.event_type === "duplicate_event.ignored").length;
  return (
    <div className="stack">
      <section>
        <div className="eyebrow">Call inspection</div>
        <h1 style={{fontSize: 40}}>{call_id}</h1>
      </section>
      <div className="grid three">
        <div className="card"><h3>Status</h3><div className="metric">{call.status}</div></div>
        <div className="card"><h3>Agent</h3><div className="metric small-text">{call.agent_name ?? "Default agent"}</div></div>
        <div className="card"><h3>Duplicate events ignored</h3><div className="metric">{duplicates}</div></div>
      </div>
      <PipelineState stage={call.current_stage} corked={call.corked} corkReason={call.cork_reason} queueDepths={{}} />
      <CallTimeline events={eventList} />
    </div>
  );
}
