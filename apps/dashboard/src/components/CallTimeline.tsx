import type {PipelineEvent} from "@/lib/api";

export function CallTimeline({events}: {events: PipelineEvent[]}) {
  return (
    <div className="card">
      <h2>Call timeline</h2>
      <div className="event-feed">
        {events.map((event) => (
          <div className="event" key={event.event_id}>
            <span className="mono muted">
              {new Date(event.created_at ?? event.timestamp ?? "").toLocaleTimeString()}
            </span>
            <span>
              <strong className="event-name">{event.event_type}</strong>
              <div className="mono muted">{JSON.stringify(event.payload)}</div>
            </span>
            <span className="mono muted">{event.trace_id?.slice(0, 10) ?? "no trace"}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

