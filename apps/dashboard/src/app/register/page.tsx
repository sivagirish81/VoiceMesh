"use client";

import {FormEvent, useState} from "react";
import Link from "next/link";
import {useRouter} from "next/navigation";
import {fetchJson} from "@/lib/api";

export default function RegisterPage() {
  const router = useRouter();
  const [form, setForm] = useState({
    name: "",
    email: "",
    password: "",
    organization_name: "",
  });
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  function update(field: keyof typeof form, value: string) {
    setForm((current) => ({...current, [field]: value}));
  }

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError("");
    setLoading(true);
    try {
      await fetchJson("/auth/register", {
        method: "POST",
        body: JSON.stringify(form),
      });
      router.push("/");
      router.refresh();
    } catch (exc) {
      const message = exc instanceof Error ? exc.message : "Registration failed";
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
        <div className="eyebrow">Create workspace</div>
        <h1>Register</h1>
        <p className="muted">
          Create a local organization, owner user, and starter voice agent.
        </p>
        <form className="stack" onSubmit={onSubmit}>
          <label className="field">
            <span>Your name</span>
            <input
              required
              value={form.name}
              onChange={(event) => update("name", event.target.value)}
            />
          </label>
          <label className="field">
            <span>Work email</span>
            <input
              required
              type="email"
              value={form.email}
              onChange={(event) => update("email", event.target.value)}
            />
          </label>
          <label className="field">
            <span>Organization name</span>
            <input
              required
              value={form.organization_name}
              onChange={(event) => update("organization_name", event.target.value)}
            />
          </label>
          <label className="field">
            <span>Password</span>
            <input
              required
              minLength={8}
              type="password"
              value={form.password}
              onChange={(event) => update("password", event.target.value)}
            />
          </label>
          {error && <div className="error">{error}</div>}
          <button className="button primary" disabled={loading} type="submit">
            {loading ? "Creating..." : "Create workspace"}
          </button>
          <p className="muted">
            Already have an account? <Link className="event-name" href="/login">Sign in</Link>
          </p>
        </form>
      </div>
    </section>
  );
}
