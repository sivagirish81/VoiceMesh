import {CallTimeline} from "@/components/CallTimeline";
import {PipelineState} from "@/components/PipelineState";
import {Call, PipelineEvent, SERVER_API_URL} from "@/lib/api";

async function load<T>(path: string): Promise<T | null> {
  try {
    const response = await fetch(`${SERVER_API_URL}${path}`, {cache: "no-store"});
    return response.ok ? response.json() : null;
  } catch {
    return null;
  }
}

export default async function CallDetail({params}: {params: Promise<{call_id: string}>}) {
  const {call_id} = await params;
  const [call, events] = await Promise.all([
    load<Call>(`/calls/${call_id}`),
    load<PipelineEvent[]>(`/calls/${call_id}/events`),
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
        <div className="card"><h3>Duplicate events ignored</h3><div className="metric">{duplicates}</div></div>
        <div className="card"><h3>Trace search</h3><a className="button" href="http://localhost:16686" target="_blank">Open Jaeger</a></div>
      </div>
      <PipelineState stage={call.current_stage} corked={call.corked} corkReason={call.cork_reason} queueDepths={{}} />
      <CallTimeline events={eventList} />
    </div>
  );
}
