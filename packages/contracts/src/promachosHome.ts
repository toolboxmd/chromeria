import * as Schema from "effect/Schema";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";

import { EnvironmentAuthorizationError } from "./auth.ts";
import { ProjectId, TrimmedNonEmptyString } from "./baseSchemas.ts";

/**
 * Creates a starter Promachos home on the environment's own machine (toolboxmd/chromeria#139):
 * the folder, a git repository, a generic `AGENTS.md` and a `CLAUDE.md`, then its project.
 * Anything already there is kept, so a failed attempt can simply be retried.
 */
export const PROMACHOS_HOME_WS_METHODS = {
  create: "promachos.createHome",
} as const;

/** The folder a new home suggests; `~` expands on the environment's machine. */
export const DEFAULT_PROMACHOS_HOME_PATH = "~/promachos";

export class PromachosHomeError extends Schema.TaggedError<PromachosHomeError>()(
  "PromachosHomeError",
  { detail: Schema.String },
) {
  override get message(): string {
    return this.detail;
  }
}

export const PromachosHomeCreateInput = Schema.Struct({ path: TrimmedNonEmptyString });
export type PromachosHomeCreateInput = typeof PromachosHomeCreateInput.Type;

export const PromachosHomeCreateResult = Schema.Struct({
  projectId: ProjectId,
  workspaceRoot: TrimmedNonEmptyString,
});
export type PromachosHomeCreateResult = typeof PromachosHomeCreateResult.Type;

export const PromachosHomeRpcGroup = RpcGroup.make(
  Rpc.make(PROMACHOS_HOME_WS_METHODS.create, {
    payload: PromachosHomeCreateInput,
    success: PromachosHomeCreateResult,
    error: Schema.Union([PromachosHomeError, EnvironmentAuthorizationError]),
  }),
);
