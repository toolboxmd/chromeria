import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  type OrchestrationV2ProviderThread,
  type ProviderInstanceConfig,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as DateTime from "effect/DateTime";

import { openCode2SessionRebinds } from "./opencode2SessionRebind.ts";

const now = DateTime.makeUnsafe("2026-11-18T00:00:00.000Z");
const opencode = ProviderDriverKind.make("opencode");

const providerThread = (
  instance: string,
  thread: string,
  providerSessionId: ProviderSessionId,
): { readonly threadId: ThreadId; readonly providerThread: OrchestrationV2ProviderThread } => ({
  threadId: ThreadId.make(thread),
  providerThread: {
    id: ProviderThreadId.make(`provider-thread:${thread}`),
    driver: opencode,
    providerInstanceId: ProviderInstanceId.make(instance),
    providerSessionId,
    appThreadId: ThreadId.make(thread),
    ownerNodeId: null,
    nativeThreadRef: { driver: opencode, nativeId: `ses_${thread}`, strength: "strong" },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  },
});
const shared = (instance: string) =>
  IdAllocator.derive.providerSession({ providerInstanceId: ProviderInstanceId.make(instance) });

it("gives each local OpenCode thread on its instance's shared session its own session", () => {
  const instances: Record<string, ProviderInstanceConfig> = {
    "opencode-work": { driver: opencode, config: { serverUrl: "" } },
    "opencode-remote": { driver: opencode, config: { serverUrl: "http://127.0.0.1:4096" } },
    codex: { driver: ProviderDriverKind.make("codex") },
  };
  const rows = [
    // The built-in instance has no settings entry until the user edits it.
    providerThread("opencode", "a", shared("opencode")),
    providerThread("opencode", "b", shared("opencode")),
    providerThread("opencode-work", "c", shared("opencode-work")),
    providerThread("opencode-remote", "d", shared("opencode-remote")),
    providerThread("codex", "e", shared("codex")),
    providerThread("opencode", "f", ProviderSessionId.make("provider-session:own")),
    providerThread("opencode", "g", shared("opencode-work")),
  ];
  const rebound = openCode2SessionRebinds(rows, instances);
  assert.deepEqual(
    rebound.map(({ threadId }) => threadId),
    ["a", "b", "c"],
  );
  const ids = rebound.map(({ providerThread }) => providerThread.providerSessionId);
  assert.equal(new Set(ids).size, 3);
  for (const id of ids) assert.notInclude(String(id), ":shared");
  for (const [index, { providerThread: rebind }] of rebound.entries()) {
    const original = rows[index]!.providerThread;
    assert.deepEqual(rebind, { ...original, providerSessionId: rebind.providerSessionId });
  }
  // Rerunning the backfill finds nothing left to rebind.
  assert.lengthOf(
    openCode2SessionRebinds(
      rebound.map(({ threadId, providerThread }) => ({ threadId, providerThread })),
      instances,
    ),
    0,
  );
});
