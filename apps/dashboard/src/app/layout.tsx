import type { Metadata } from "next";
import {WorkspaceNav} from "@/components/WorkspaceNav";
import "./globals.css";

export const metadata: Metadata = {
  title: "VoiceMesh",
  description: "Real-time voice AI reliability workspace",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>
        <main className="shell">
          <WorkspaceNav />
          {children}
        </main>
      </body>
    </html>
  );
}
