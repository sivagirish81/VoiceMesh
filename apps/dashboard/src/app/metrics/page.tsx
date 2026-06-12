import {LatencyChart, StageMetric} from "@/components/LatencyChart";
import {SERVER_API_URL} from "@/lib/api";

async function getMetrics(): Promise<StageMetric[]> {
  try {
    const response = await fetch(`${SERVER_API_URL}/metrics/summary`, {cache: "no-store"});
    if (!response.ok) return [];
    const body = await response.json();
    return body.stages.map((row: Record<string, string | number>) => ({
      ...row,
      p50: Number(row.p50),
      p95: Number(row.p95),
      p99: Number(row.p99),
      samples: Number(row.samples),
    }));
  } catch {
    return [];
  }
}

export default async function MetricsPage() {
  const stages = await getMetrics();
  return (
    <div className="stack">
      <section>
        <div className="eyebrow">Latency and reliability signals</div>
        <h1 style={{fontSize: 46}}>Metrics</h1>
      </section>
      <LatencyChart data={stages} />
      <div className="grid three">
        {stages.map((stage) => (
          <div className="card" key={stage.stage}>
            <h3>{stage.stage}</h3>
            <div className="metric">{Math.round(stage.p95)} <small>ms p95</small></div>
            <p>{stage.samples} persisted samples · p99 {Math.round(stage.p99)} ms</p>
          </div>
        ))}
        {stages.length === 0 && <div className="card"><p>Run a call to populate latency percentiles.</p></div>}
      </div>
    </div>
  );
}
