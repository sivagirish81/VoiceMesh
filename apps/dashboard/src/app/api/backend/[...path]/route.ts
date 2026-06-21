import {NextRequest, NextResponse} from "next/server";
import {SERVER_API_URL} from "@/lib/api";

type RouteContext = {
  params: Promise<{path: string[]}>;
};

const HOP_BY_HOP_HEADERS = new Set([
  "connection",
  "content-encoding",
  "content-length",
  "keep-alive",
  "transfer-encoding",
]);

async function proxy(request: NextRequest, context: RouteContext) {
  const {path} = await context.params;
  const upstreamUrl = new URL(`/${path.join("/")}`, SERVER_API_URL);
  upstreamUrl.search = request.nextUrl.search;

  const headers = new Headers();
  const contentType = request.headers.get("content-type");
  const cookie = request.headers.get("cookie");
  if (contentType) headers.set("content-type", contentType);
  if (cookie) headers.set("cookie", cookie);

  const method = request.method.toUpperCase();
  const body = method === "GET" || method === "HEAD" ? undefined : await request.text();
  const upstream = await fetch(upstreamUrl, {
    method,
    headers,
    body,
    cache: "no-store",
  });

  const responseHeaders = new Headers();
  upstream.headers.forEach((value, key) => {
    if (!HOP_BY_HOP_HEADERS.has(key.toLowerCase())) {
      responseHeaders.set(key, value);
    }
  });

  return new NextResponse(await upstream.text(), {
    status: upstream.status,
    headers: responseHeaders,
  });
}

export async function GET(request: NextRequest, context: RouteContext) {
  return proxy(request, context);
}

export async function POST(request: NextRequest, context: RouteContext) {
  return proxy(request, context);
}

export async function PATCH(request: NextRequest, context: RouteContext) {
  return proxy(request, context);
}

export async function DELETE(request: NextRequest, context: RouteContext) {
  return proxy(request, context);
}
