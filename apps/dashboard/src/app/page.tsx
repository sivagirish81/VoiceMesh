import Link from "next/link";

export default function Home() {
  return (
    <>
      <section className="hero">
        <div className="eyebrow">Real-time systems, under pressure</div>
        <h1>A reliability lab for the voice pipeline between hello and heard.</h1>
        <p>
          VoiceMesh runs a real browser microphone through VAD, OpenAI transcription,
          streaming generation, speech synthesis, and browser playback. Kafka, Temporal,
          Postgres, and OpenTelemetry expose what happens when the happy path stops being happy.
        </p>
        <div className="actions">
          <Link className="button primary" href="/demo">Start a live call</Link>
          <Link className="button" href="/calls">Inspect recent calls</Link>
        </div>
      </section>
      <section className="grid three">
        <div className="card">
          <h3>Streaming plane</h3>
          <div className="metric">Kafka</div>
          <p>High-throughput pipeline and provider events with durable replay.</p>
        </div>
        <div className="card">
          <h3>Lifecycle plane</h3>
          <div className="metric">Temporal</div>
          <p>Durable call state, timeouts, degradation, and worker-crash recovery.</p>
        </div>
        <div className="card">
          <h3>Failure plane</h3>
          <div className="metric">Visible</div>
          <p>Corking, duplicates, provider latency, and database failures are first-class signals.</p>
        </div>
      </section>
    </>
  );
}

