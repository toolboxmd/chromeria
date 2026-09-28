/**
 * Child threads on mobile (toolboxmd/chromeria#59): the store mobile app has
 * no Agents panel, so every child thread would land in its thread list. Mobile
 * connections get the shell without child threads; web and desktop keep them
 * for the Agents panel. Upstream Orchestrator V2 hides subagent threads on
 * mobile natively, so this goes away when the fork moves to V2.
 */
import type { OrchestrationShellSnapshot, OrchestrationShellStreamItem } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { isSubagentThreadId } from "./subagentThreadId.ts";

export const hidesChildThreads = (surface: string | null | undefined) => surface === "mobile";

export const withoutChildThreads = (
  snapshot: OrchestrationShellSnapshot,
): OrchestrationShellSnapshot => ({
  ...snapshot,
  threads: snapshot.threads.filter((thread) => !isSubagentThreadId(thread.id)),
});

const isChildThreadEvent = (item: OrchestrationShellStreamItem) =>
  (item.kind === "thread-upserted" && isSubagentThreadId(item.thread.id)) ||
  (item.kind === "thread-removed" && isSubagentThreadId(item.threadId));

export const withoutChildThreadItems = <A extends OrchestrationShellStreamItem, E, R>(
  stream: Stream.Stream<A, E, R>,
): Stream.Stream<A, E, R> =>
  stream.pipe(
    Stream.filter((item) => !isChildThreadEvent(item)),
    Stream.map((item) =>
      item.kind === "snapshot" ? { ...item, snapshot: withoutChildThreads(item.snapshot) } : item,
    ),
  );

/** A shell snapshot as a client on `surface` should see it. */
export const shellSnapshotFor =
  (surface: string | null | undefined) => (snapshot: OrchestrationShellSnapshot) =>
    hidesChildThreads(surface) ? withoutChildThreads(snapshot) : snapshot;

/** A `subscribeShell` stream as a client on `surface` should see it. */
export const shellStreamFor =
  (surface: string | null | undefined) =>
  <A extends OrchestrationShellStreamItem, E, R>(stream: Stream.Stream<A, E, R>) =>
    hidesChildThreads(surface) ? withoutChildThreadItems(stream) : stream;

/**
 * Looks up the surface an auth session's client reported on its last WebSocket
 * connect. HTTP requests carry no surface, but clients connect the socket
 * before they load the shell over HTTP. Null when unknown.
 */
export const makeSessionClientSurface = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return (sessionId: string) =>
    sql<{ readonly surface: string | null }>`
      SELECT client_surface AS surface FROM auth_sessions WHERE session_id = ${sessionId}
    `.pipe(
      Effect.map((rows) => rows[0]?.surface ?? null),
      Effect.orElseSucceed(() => null),
    );
});
