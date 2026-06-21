import {AuthMe} from "@/lib/api";
import {serverFetchJson} from "@/lib/serverApi";

export default async function SettingsPage() {
  const me = await serverFetchJson<AuthMe>("/auth/me");
  return (
    <div className="stack">
      <section>
        <div className="eyebrow">Workspace settings</div>
        <h1>Settings</h1>
        <p className="muted">Organization and user settings for the local VoiceMesh workspace.</p>
      </section>
      <div className="grid two">
        <div className="card">
          <h3>Organization</h3>
          <div className="metric small-text">{me.organization.name}</div>
          <p className="mono muted">{me.organization.id}</p>
        </div>
        <div className="card">
          <h3>User</h3>
          <div className="metric small-text">{me.user.email}</div>
          <p className="muted">Role: {me.role}</p>
        </div>
      </div>
    </div>
  );
}
