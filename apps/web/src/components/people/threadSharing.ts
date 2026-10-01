import { request } from "@t3tools/client-runtime/rpc";
import { createEnvironmentCommand } from "@t3tools/client-runtime/state/runtime";
import { CommandId, ORCHESTRATION_WS_METHODS, type ThreadId } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import { connectionAtomRuntime } from "../../connection/runtime";
import type { ThreadSharingAction } from "./personView";

export type ThreadSharingInput = ThreadSharingAction & {
  readonly threadId: ThreadId;
  /** The acting person. The server replaces it with the session's own label. */
  readonly actor: string;
};

/** Dispatches `thread.share`, `thread.unshare` or `thread.leave` on one environment. */
export const threadSharingCommand = createEnvironmentCommand(connectionAtomRuntime, {
  label: "environment-data:commands:thread:sharing",
  execute: (input: ThreadSharingInput) =>
    Effect.gen(function* () {
      const crypto = yield* Crypto.Crypto;
      const commandId = yield* crypto.randomUUIDv4.pipe(Effect.orDie, Effect.map(CommandId.make));
      const createdAt = DateTime.formatIso(yield* DateTime.now);
      return yield* request(ORCHESTRATION_WS_METHODS.dispatchCommand, {
        ...input,
        commandId,
        createdAt,
      });
    }),
});
