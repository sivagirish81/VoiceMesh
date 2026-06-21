"use client";

import Link from "next/link";
import {usePathname, useRouter} from "next/navigation";
import {useEffect, useState} from "react";
import {AuthMe, fetchJson} from "@/lib/api";

export function WorkspaceNav() {
  const pathname = usePathname();
  const router = useRouter();
  const [me, setMe] = useState<AuthMe | null>(null);

  useEffect(() => {
    let cancelled = false;
    if (pathname === "/login") return;
    fetchJson<AuthMe>("/auth/me")
      .then((value) => {
        if (!cancelled) setMe(value);
      })
      .catch(() => {
        if (!cancelled) setMe(null);
      });
    return () => {
      cancelled = true;
    };
  }, [pathname]);

  async function logout() {
    await fetchJson("/auth/logout", {method: "POST"}).catch(() => undefined);
    router.push("/login");
    router.refresh();
  }

  return (
    <nav className="nav">
      <Link className="brand" href="/">
        <span className="brand-mark" />
        <span>VoiceMesh</span>
      </Link>
      {pathname !== "/login" && (
        <>
          <div className="nav-links">
            <Link href="/">Dashboard</Link>
            <Link href="/agents">Agents</Link>
            <Link href="/calls">Calls</Link>
            <Link href="/billing">Billing</Link>
            <Link href="/metrics">Observability</Link>
            <Link href="/settings">Settings</Link>
          </div>
          <div className="nav-user">
            <span>{me?.organization.name ?? "Workspace"}</span>
            <button className="button small" onClick={logout}>Logout</button>
          </div>
        </>
      )}
    </nav>
  );
}
