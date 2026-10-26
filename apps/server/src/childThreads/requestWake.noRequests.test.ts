import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { CommandReceiptStoreV2 } from "../orchestration-v2/CommandReceiptStore.ts";
import {
  ProjectionStoreV2,
  type ProjectionStoreV2Shape,
} from "../orchestration-v2/ProjectionStore.ts";
import { ThreadCommandExecutor } from "../orchestration-v2/ThreadCommandExecutor.ts";
import { reconcileChildRequests } from "./requestWake.ts";

// Startup reconciles every subagent thread. Loading each child's ancestors made
// boot read a large parent thread once per child and run out of memory.
it.effect("a child with no deliverable requests never loads its ancestors", () =>
  Effect.gen(function* () {
    const child = ThreadId.make("child");
    const parent = ThreadId.make("parent");
    const reads: Array<string> = [];
    const projections = {
      getThreadRecords: (threadId: ThreadId) => {
        reads.push(`records:${threadId}`);
        if (threadId !== child) return Effect.die(`loaded ancestor records ${threadId}`);
        return Effect.succeed({
          thread: { lineage: { relationshipToParent: "subagent", parentThreadId: parent } },
          runtimeRequests: [
            { id: "tool", kind: "dynamic_tool_call", status: "pending" },
            { id: "auth", kind: "auth_refresh", status: "pending" },
          ],
        });
      },
      getThreadShell: (threadId: ThreadId) => Effect.die(`loaded ancestor shell ${threadId}`),
      getThread: (threadId: ThreadId) => Effect.die(`loaded thread ${threadId}`),
    } as unknown as ProjectionStoreV2Shape;
    const executor = {
      withLock: () => Effect.die("took a parent lock"),
    } as never;

    yield* reconcileChildRequests(
      child,
      () => Effect.die("dispatched"),
      () => Effect.die("reconciled native question"),
    ).pipe(
      Effect.provideService(ProjectionStoreV2, projections),
      Effect.provideService(ThreadCommandExecutor, executor),
      Effect.provideService(CommandReceiptStoreV2, {} as never),
    );

    assert.deepStrictEqual(reads, [`records:${child}`]);
  }),
);
