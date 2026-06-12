import type { Metadata } from "next";
import Link from "next/link";
import "./globals.css";

export const metadata: Metadata = {
  title: "VoiceMesh Reliability Lab",
  description: "Production-inspired live voice pipeline reliability laboratory",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>
        <main className="shell">
          <nav className="nav">
            <Link className="brand" href="/">
              <span className="brand-mark" />
              <span>VoiceMesh</span>
            </Link>
            <div className="nav-links">
              <Link href="/demo">Live Demo</Link>
              <Link href="/calls">Calls</Link>
              <Link href="/metrics">Metrics</Link>
              <a href="http://localhost:8080" target="_blank">Temporal</a>
              <a href="http://localhost:16686" target="_blank">Jaeger</a>
            </div>
          </nav>
          {children}
        </main>
      </body>
    </html>
  );
}

