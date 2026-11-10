import { ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export class ForkProviderlessMessageReadError extends Schema.TaggedError<ForkProviderlessMessageReadError>()(
  "ForkProviderlessMessageReadError",
  { threadId: ThreadId, cause: Schema.Defect() },
) {}

/** Registered transcripts still use serialized dispatch, but start no provider. */
export class ForkProviderlessMessages extends Context.Reference<{
  readonly contains: (
    threadId: ThreadId,
  ) => Effect.Effect<boolean, ForkProviderlessMessageReadError>;
}>("t3/fork/ForkProviderlessMessages", {
  defaultValue: () => ({ contains: () => Effect.succeed(false) }),
}) {}
