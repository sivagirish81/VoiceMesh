type Props = {
  stage: string;
  corked: boolean;
  corkReason?: string | null;
  queueDepths: Record<string, number>;
};

const stages = ["transport", "vad", "stt", "llm", "tts", "transport"];

export function PipelineState({stage, corked, corkReason, queueDepths}: Props) {
  return (
    <div className="card">
      <div className="row">
        <h2>Pipeline state</h2>
        <span className={`status ${corked ? "corked" : ""}`}>
          {corked ? "corked" : "uncorked"}
        </span>
      </div>
      <div className="mono muted" style={{marginBottom: 18}}>
        {stages.map((item, index) => (
          <span key={`${item}-${index}`} style={{color: item === stage ? "var(--cyan)" : undefined}}>
            {index ? "  →  " : ""}{item.toUpperCase()}
          </span>
        ))}
      </div>
      {Object.keys(queueDepths).length === 0 && <p>No queue activity yet.</p>}
      {Object.entries(queueDepths).map(([name, depth]) => (
        <div className="queue" key={name}>
          <span className="mono">{name}</span>
          <div className="bar"><span style={{width: `${Math.min(depth * 10, 100)}%`}} /></div>
          <strong>{depth}</strong>
        </div>
      ))}
      {corkReason && <div className="callout">{corkReason}</div>}
    </div>
  );
}

