import {
  AuthSessionId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationShellStreamItem,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import * as AuthSessions from "../../../persistence/AuthSessions.ts";
import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { makeSessionClientSurface, shellStreamFor } from "./mobileShell.ts";

const now = "2026-01-01T00:00:00.000Z";
const userThreadId = ThreadId.make("thread-user");
const childThreadId = ThreadId.make("sub.thread-user.child1");

const threadShell = (id: ThreadId): OrchestrationThreadShell => ({
  id,
  projectId: ProjectId.make("project-1"),
  title: "Thread",
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5-codex" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  pullRequests: [],
  latestTurn: null,
  createdAt: now,
  updatedAt: now,
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  session: null,
  latestUserMessageAt: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  hasActionableProposedPlan: false,
});

const items: ReadonlyArray<OrchestrationShellStreamItem> = [
  {
    kind: "snapshot",
    snapshot: {
      snapshotSequence: 1,
      projects: [],
      threads: [threadShell(userThreadId), threadShell(childThreadId)],
      updatedAt: now,
    },
  },
  { kind: "synchronized" },
  { kind: "thread-upserted", sequence: 2, thread: threadShell(childThreadId) },
  { kind: "thread-upserted", sequence: 3, thread: threadShell(userThreadId) },
  { kind: "thread-removed", sequence: 4, threadId: childThreadId },
  { kind: "thread-removed", sequence: 5, threadId: userThreadId },
];

const threadIdsSeen = (surface: string | null) =>
  Stream.fromIterable(items).pipe(
    shellStreamFor(surface),
    Stream.runCollect,
    Effect.map((collected) =>
      Array.from(collected).flatMap((item) => {
        switch (item.kind) {
          case "snapshot":
            return item.snapshot.threads.map((thread) => `snapshot:${thread.id}`);
          case "thread-upserted":
            return [`upserted:${item.thread.id}`];
          case "thread-removed":
            return [`removed:${item.threadId}`];
          default:
            return [item.kind];
        }
      }),
    ),
  );

describe("mobile shell", () => {
  it.effect("drops child threads from mobile shell streams", () =>
    Effect.gen(function* () {
      assert.deepEqual(yield* threadIdsSeen("mobile"), [
        `snapshot:${userThreadId}`,
        "synchronized",
        `upserted:${userThreadId}`,
        `removed:${userThreadId}`,
      ]);
    }),
  );

  it.effect("keeps child threads for web, desktop and unknown clients", () =>
    Effect.gen(function* () {
      const everything = [
        `snapshot:${userThreadId}`,
        `snapshot:${childThreadId}`,
        "synchronized",
        `upserted:${childThreadId}`,
        `upserted:${userThreadId}`,
        `removed:${childThreadId}`,
        `removed:${userThreadId}`,
      ];
      assert.deepEqual(yield* threadIdsSeen("web"), everything);
      assert.deepEqual(yield* threadIdsSeen("desktop"), everything);
      assert.deepEqual(yield* threadIdsSeen(null), everything);
    }),
  );

  it.effect("reads the surface a session last connected with", () =>
    Effect.gen(function* () {
      const sessions = yield* AuthSessions.AuthSessionRepository;
      const sessionClientSurface = yield* makeSessionClientSurface;
      const sessionId = AuthSessionId.make("session-phone");
      yield* sessions.create({
        sessionId,
        subject: "phone",
        scopes: ["access:read"],
        method: "bearer-access-token",
        client: {
          label: null,
          ipAddress: null,
          userAgent: null,
          deviceType: "unknown",
          os: null,
          browser: null,
        },
        issuedAt: DateTime.makeUnsafe("2026-06-20T00:00:00.000Z"),
        expiresAt: DateTime.makeUnsafe("2027-06-20T00:00:00.000Z"),
      });

      assert.equal(yield* sessionClientSurface(sessionId), null);
      yield* sessions.setClientConnection({ sessionId, surface: "mobile", appVersion: null });
      assert.equal(yield* sessionClientSurface(sessionId), "mobile");
      assert.equal(yield* sessionClientSurface("session-unknown"), null);
    }).pipe(Effect.provide(AuthSessions.layer.pipe(Layer.provideMerge(SqlitePersistenceMemory)))),
  );
});
