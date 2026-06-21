"use client";

import {FormEvent, useState} from "react";
import Link from "next/link";
import {useRouter} from "next/navigation";
import {fetchJson} from "@/lib/api";

export default function LoginPage() {
  const router = useRouter();
  const [email, setEmail] = useState("admin@voicemesh.local");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError("");
    setLoading(true);
    try {
      await fetchJson("/auth/login", {
        method: "POST",
        body: JSON.stringify({email, password}),
      });
      router.push("/");
      router.refresh();
    } catch (exc) {
      const message = exc instanceof Error ? exc.message : "Login failed";
      setError(
        message.includes("Failed to fetch")
          ? "Could not reach the API. Use http://localhost:3000 and make sure the API is running on port 8000."
          : message,
      );
    } finally {
      setLoading(false);
    }
  }

  return (
    <section className="auth-page">
      <div className="card auth-card">
        <div className="eyebrow">VoiceMesh workspace</div>
        <h1>Sign in</h1>
        <p className="muted">
          Use the seeded local admin from your environment to manage organizations,
          voice agents, calls, and billing projections.
        </p>
        <form className="stack" onSubmit={onSubmit}>
          <label className="field">
            <span>Email</span>
            <input value={email} onChange={(event) => setEmail(event.target.value)} />
          </label>
          <label className="field">
            <span>Password</span>
            <input
              type="password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              placeholder="VOICEMESH_ADMIN_PASSWORD"
            />
          </label>
          {error && <div className="error">{error}</div>}
          <button className="button primary" disabled={loading} type="submit">
            {loading ? "Signing in..." : "Sign in"}
          </button>
          <p className="muted">
            New workspace? <Link className="event-name" href="/register">Create an account</Link>
          </p>
        </form>
      </div>
    </section>
  );
}
