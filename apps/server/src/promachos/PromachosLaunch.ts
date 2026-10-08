import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Prism from "../prism/PrismService.ts";
import * as Adapters from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Providers from "../provider/ProviderRegistry.ts";

export class PromachosLaunchError extends Schema.TaggedError<PromachosLaunchError>()(
  "PromachosLaunchError",
  {
    reason: Schema.Literals(["initial-only", "routing", "read-thread"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    switch (this.reason) {
      case "initial-only":
        return "Prism can select the Promachos model only when creating a new top-level conversation.";
      case "routing":
        return "No eligible Promachos model with usage headroom.";
      case "read-thread":
        return "Could not verify the new Promachos conversation.";
    }
  }
}
export type PromachosLaunchInput = ThreadLaunch.ThreadLaunchInput & {
  readonly prismRole?: "promachos";
};

export class PromachosLaunch extends Context.Service<
  PromachosLaunch,
  {
    readonly prepare: (
      input: PromachosLaunchInput,
    ) => Effect.Effect<ThreadLaunch.ThreadLaunchInput, PromachosLaunchError>;
  }
>()("t3/promachos/PromachosLaunch") {}

const make = Effect.gen(function* () {
  const threads = yield* Threads.ThreadManagementService;
  const receipts = yield* Receipts.CommandReceiptStoreV2;
  const prism = yield* Prism.PrismService;
  const adapters = yield* Adapters.ProviderAdapterRegistryV2;
  const providers = yield* Providers.ProviderRegistry;
  const prepare = Effect.fn("PromachosLaunch.prepare")(function* (input: PromachosLaunchInput) {
    const { prismRole, ...launch } = input;
    if (prismRole === undefined) return launch;
    if (input.reuseExistingThread === true || input.initialMessage === undefined) {
      return yield* new PromachosLaunchError({ reason: "initial-only" });
    }
    const receipt = yield* receipts
      .getByCommandId(input.commandId)
      .pipe(Effect.mapError((cause) => new PromachosLaunchError({ reason: "read-thread", cause })));
    const threadId =
      input.threadId ?? (Option.isSome(receipt) ? receipt.value.threadId : undefined);
    const existing =
      threadId === undefined
        ? null
        : yield* threads
            .getThreadShell(threadId)
            .pipe(
              Effect.mapError(
                (cause) => new PromachosLaunchError({ reason: "read-thread", cause }),
              ),
            );
    if (existing !== null) {
      // A transport retry replays the upstream launch receipt, never today's kit/settings.
      if (
        Option.isSome(receipt) &&
        receipt.value.status === "accepted" &&
        receipt.value.commandType === "thread.create" &&
        receipt.value.threadId === existing.id &&
        existing.projectId === input.projectId &&
        existing.lineage.relationshipToParent !== "subagent"
      ) {
        return { ...launch, modelSelection: existing.modelSelection };
      }
      return yield* new PromachosLaunchError({ reason: "initial-only" });
    }
    const selected = yield* prism
      .resolve({
        projectId: input.projectId,
        role: "promachos",
        inherited: input.modelSelection,
        validate: (selection) =>
          Prism.validateLaunchSelection(selection).pipe(
            Effect.provideService(Adapters.ProviderAdapterRegistryV2, adapters),
            Effect.provideService(Providers.ProviderRegistry, providers),
          ),
      })
      .pipe(Effect.mapError((cause) => new PromachosLaunchError({ reason: "routing", cause })));
    return {
      ...launch,
      modelSelection: selected.modelSelection,
      initialMessage: {
        ...input.initialMessage,
        text: [selected.kitText, input.initialMessage.text].filter(Boolean).join("\n\n"),
      },
    };
  });
  return PromachosLaunch.of({ prepare });
});
export const layer = Layer.effect(PromachosLaunch, make);
