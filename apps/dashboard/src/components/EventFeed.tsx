import type {PipelineEvent} from "@/lib/api";

export function EventFeed({events}: {events: PipelineEvent[]}) {
  return (
    <div className="card">
      <div className="row">
        <h2>Pipeline events</h2>
        <span className="mono muted">{events.length} observed</span>
      </div>
      <div className="event-feed">
        {events.length === 0 && <p>Events will appear as the call moves through the pipeline.</p>}
        {[...events].reverse().map((event) => (
          <div className="event" key={`${event.event_id}-${event.sequence_number}`}>
            <span className="mono muted">#{event.sequence_number}</span>
            <span className="event-name">{event.event_type}</span>
            <span className="mono muted">{event.stage}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

