"use client";

import {Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis} from "recharts";

export type StageMetric = {stage: string; p50: number; p95: number; p99: number; samples: number};

export function LatencyChart({data}: {data: StageMetric[]}) {
  return (
    <div className="card" style={{height: 390}}>
      <h2>Stage latency percentiles</h2>
      <ResponsiveContainer width="100%" height={310}>
        <BarChart data={data}>
          <CartesianGrid stroke="#203845" vertical={false} />
          <XAxis dataKey="stage" stroke="#8fa9b7" />
          <YAxis stroke="#8fa9b7" unit="ms" />
          <Tooltip contentStyle={{background: "#0d1a24", border: "1px solid #203845"}} />
          <Bar dataKey="p50" fill="#41d9c7" />
          <Bar dataKey="p95" fill="#61a8ff" />
          <Bar dataKey="p99" fill="#f4b860" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

