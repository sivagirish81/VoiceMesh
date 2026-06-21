import {cookies} from "next/headers";
import {redirect} from "next/navigation";
import {SERVER_API_URL} from "./api";

export async function serverFetchJson<T>(path: string): Promise<T> {
  const cookieStore = await cookies();
  const response = await fetch(`${SERVER_API_URL}${path}`, {
    headers: {cookie: cookieStore.toString()},
    cache: "no-store",
  });
  if (response.status === 401) redirect("/login");
  if (!response.ok) throw new Error(`${response.status} ${await response.text()}`);
  return response.json() as Promise<T>;
}
