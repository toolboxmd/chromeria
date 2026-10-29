import {
  registerToolEffect,
  classifyToolEffect,
  type McpToolEffectClassification,
} from "./McpToolEffects.ts";
import {
  type OrchestrationV2LinkOrigin,
  OrchestratorMcpFailure,
  type ProviderInteractionMode,
  type RuntimeMode,
  type ThreadId,
} from "@t3tools/contracts";
import * as McpRouting from "./McpRouting.ts";
import { checkOutsideGrant } from "./outsideGrant.ts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import * as Ref from "effect/Ref";
import * as Struct from "effect/Struct";
import type * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import type { Tool, Toolkit } from "effect/ai";

import {
  DispatchModeLimit,
  type DispatchModeRefusal,
} from "../orchestration-v2/DispatchModeLimit.ts";
import { assertNotLinked, assertSameLink, callerLinkOrigin } from "./linkOrigin.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import { resolveInteractionMode, resolveRuntimeMode } from "./delegatedTaskModes.ts";
import {
  assertFullAccess,
  assertLiveCaller,
  assertTargetWithinLimits,
  type Caller,
  loadCaller,
  readCaller,
  unavailable,
} from "./threadAccess.ts";

/** Private constructors only enforce types; the token also blocks runtime construction. */
const builtHere: unique symbol = Symbol("t3/mcp/McpToolAccess/builtHere");

const refuseOutsideConstruction = (token: symbol) => {
  if (token !== builtHere) {
    throw new TypeError("Only McpToolAccess builds MCP tool declarations and handler layers.");
  }
};

let declare: <P, A, E, R>(
  handle: (params: P) => Effect.Effect<A, E, R>,
  effect?: McpToolEffectClassification,
) => Declaration<(params: P) => Effect.Effect<A, E | OrchestratorMcpFailure, R>>;

type CheckedHandlerOf<D> = D extends Declaration<infer Handler> ? Handler : never;

interface CheckedHandler extends Struct.Lambda {
  <Handler>(declaration: Declaration<Handler>): Handler;
  readonly "~lambda.out": CheckedHandlerOf<this["~lambda.in"]>;
}
let checkedHandler: CheckedHandler;

let handlersLayer: <Tools extends Record<string, Tool.Any>, EX, RX>(
  layer: Layer.Layer<Tool.HandlersFor<Tools>, EX, RX>,
) => HandlersLayer<Tools, EX, RX>;

/** t3code/no-raw-mcp-registration confines Effect's unchecked registration API to McpHttpServer. */
export class Declaration<out Handler> {
  readonly #handle: Handler;
  readonly effect: McpToolEffectClassification;
  private constructor(
    token: typeof builtHere,
    handle: Handler,
    effect: McpToolEffectClassification,
  ) {
    refuseOutsideConstruction(token);
    this.#handle = handle;
    this.effect = effect;
  }
  static {
    declare = (handle, effect = "owned") =>
      new Declaration(
        builtHere,
        (params) =>
          McpRouting.CurrentTool.pipe(
            Effect.flatMap((tool) => checkOutsideGrant(tool?.name ?? "", params)),
            Effect.andThen(McpRouting.withContext(params, handle(params))),
          ),
        effect,
      );
    checkedHandler = Struct.lambda<CheckedHandler>((declaration) => declaration.#handle);
  }
}

export const either = <P, A, E, R, A2, E2, R2>(
  useFirst: (params: P) => boolean,
  first: Declaration<(params: P) => Effect.Effect<A, E, R>>,
  second: Declaration<(params: P) => Effect.Effect<A2, E2, R2>>,
  effect: McpToolEffectClassification = "owned",
) =>
  declare(
    (params: P): Effect.Effect<A | A2, E | E2, R | R2> =>
      useFirst(params) ? checkedHandler(first)(params) : checkedHandler(second)(params),
    effect,
  );

const refuseReadOnlyClient = McpInvocationContext.McpInvocationContext.pipe(
  Effect.flatMap((scope) =>
    scope.client?.access === "read-only"
      ? Effect.fail(
          new OrchestratorMcpFailure({
            code: "capability_denied",
            message:
              "This tool changes the environment, and this MCP client was approved for read-only access.",
          }),
        )
      : Effect.void,
  ),
);

const writingCaller = refuseReadOnlyClient.pipe(
  Effect.andThen(loadCaller()),
  Effect.tap(assertLiveCaller),
);

const orchestratingCaller = refuseReadOnlyClient.pipe(
  Effect.andThen(readCaller()),
  Effect.tap(assertLiveCaller),
);

const requireThreadCaller = McpInvocationContext.McpInvocationContext.pipe(
  Effect.flatMap((scope) => McpInvocationContext.requireThreadScope(scope, "This tool")),
);

export const reads = <P, A, E, R>(handle: (params: P) => Effect.Effect<A, E, R>) =>
  declare((params: P) => McpRouting.invoke(params, handle), "read");

// Peer agents have no destination thread; opt-in keeps other caller reads from widening.
export const readsAsCaller = <P, A, E, R>(
  handle: (params: P) => Effect.Effect<A, E, R>,
  options?: {
    readonly linkedCallers?: "allowed";
    readonly effect?: McpToolEffectClassification;
  },
) =>
  declare(
    (params: P) =>
      McpInvocationContext.McpInvocationContext.pipe(
        Effect.flatMap((scope) =>
          options?.linkedCallers === "allowed" && scope.client?.linked === true
            ? scope.client.access === "read-only"
              ? Effect.fail(
                  new OrchestratorMcpFailure({
                    code: "capability_denied",
                    message: "This read requires a peer link with agent access.",
                  }),
                )
              : Effect.void
            : requireThreadCaller,
        ),
        Effect.flatMap(() => McpRouting.invoke(params, handle)),
      ),
    options?.effect ?? "read",
  );

/**
 * Acts as the calling T3 thread (its subagents, preview tabs, devices,
 * worktree) while that thread's run is live. Only an agent running inside a
 * T3 thread has one.
 */
export const actsAsCaller = <P, A, E, R>(
  handle: (params: P) => Effect.Effect<A, E, R>,
  effect: "read" | "owned" | "external-unknown" = "owned",
) =>
  declare(
    (params: P) =>
      requireThreadCaller.pipe(
        Effect.andThen(writingCaller),
        Effect.flatMap(() => McpRouting.invoke(params, handle)),
      ),
    effect,
  );

/** Resource context need not run a provider when verified incoming authority owns the action. */
export const recheckResource = Effect.fn("McpToolAccess.recheckResource")(function* (
  intent: Parameters<PeerAuthority.PeerAuthority["Service"]["recheckInvocation"]>[1],
) {
  const scope = yield* requireThreadCaller;
  yield* refuseReadOnlyClient;
  if (scope.client?.linked === true && scope.authority !== undefined) {
    const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
    if (Option.isNone(authority)) return yield* unavailable();
    return yield* authority.value.recheckInvocation(scope, intent, scope.thread.threadId);
  }
  yield* writingCaller;
  return scope;
});

export const actsOnResource = <P, A, E, R>(
  intent: Parameters<typeof recheckResource>[0],
  handle: (params: P) => Effect.Effect<A, E, R>,
  effect: "read" | "owned" | "external-unknown" = "owned",
) =>
  declare(
    (params: P) => recheckResource(intent).pipe(Effect.andThen(McpRouting.invoke(params, handle))),
    effect,
  );

/** Rechecks staged bytes without treating a verified destination context as a provider run. */
export const recheckAttachmentStaging = Effect.gen(function* () {
  const caller = yield* loadCaller();
  const { scope } = caller;
  if (
    scope.client?.linked === true &&
    scope.authority !== undefined &&
    scope.thread !== undefined
  ) {
    if (caller.caller?.archivedAt !== null)
      return yield* new OrchestratorMcpFailure({
        code: "capability_denied",
        message: "The attachment resource context is no longer available.",
      });
    const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
    if (Option.isNone(authority)) return yield* unavailable();
    yield* authority.value.checkTarget(scope, { type: "thread", threadId: scope.thread.threadId });
  } else {
    yield* assertLiveCaller(caller);
    if (scope.authority !== undefined && scope.thread !== undefined) {
      const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
      if (Option.isNone(authority)) return yield* unavailable();
      yield* authority.value.checkTarget(scope, {
        type: "thread",
        threadId: scope.thread.threadId,
      });
    }
  }
});

/** Staging is generic attachment work; its eventual native consumer owns resource capability consent. */
export const stagesAttachments = <P, A, E, R>(handle: (params: P) => Effect.Effect<A, E, R>) =>
  declare((params: P) =>
    refuseReadOnlyClient.pipe(
      Effect.andThen(readCaller()),
      Effect.andThen(recheckAttachmentStaging),
      Effect.andThen(McpRouting.invoke(params, handle)),
    ),
  );

/**
 * Changes something that belongs to no thread, such as a pending upload or a
 * scheduled task. Work a linked environment started may not: none of it
 * carries the link's origin, so none of it is that link's.
 */
export const writes = <P, A, E, R>(
  handle: (params: P) => Effect.Effect<A, E, R>,
  options?: {
    /** Whether a linked caller may make it, as for uploads its own sends need. */
    readonly linkedCallers?: "allowed";
  },
) =>
  declare((params: P) =>
    orchestratingCaller.pipe(
      Effect.tap((caller) =>
        options?.linkedCallers === "allowed"
          ? Effect.void
          : assertNotLinked(caller, "change this environment's scheduled tasks"),
      ),
      Effect.flatMap(() => McpRouting.invoke(params, handle)),
    ),
  );

/**
 * Unlike `writes` it does not need thread control; the handler checks its
 * capability, as the capability toolkits do.
 */
export const writesWithCapability = <P, A, E, R>(
  handle: (params: P) => Effect.Effect<A, E, R>,
  effect: "owned" | "external-unknown" = "owned",
  options?: { readonly linkedCallers?: "allowed" },
) =>
  declare(
    (params: P) =>
      writingCaller.pipe(
        Effect.tap((caller) =>
          options?.linkedCallers === "allowed"
            ? Effect.void
            : assertNotLinked(caller, "start work here that is not its link's"),
        ),
        Effect.flatMap(() => McpRouting.invoke(params, handle)),
      ),
    effect,
  );

export const writesThreads = <P, A, E, R>(
  threads: (
    params: P,
    scope: McpInvocationContext.McpInvocationScope,
  ) => ReadonlyArray<ThreadId | undefined>,
  handle: (params: P) => Effect.Effect<A, E, R>,
) =>
  declare((params: P) =>
    Effect.gen(function* () {
      const caller = yield* writingCaller;
      for (const threadId of McpRouting.isRemote(params, caller.scope)
        ? []
        : threads(params, caller.scope)) {
        if (threadId === undefined) continue;
        const target = yield* caller.threads
          .getThreadShell(threadId)
          .pipe(Effect.mapError(unavailable));
        if (target !== null && target.deletedAt === null) {
          yield* assertTargetWithinLimits(caller.limits, target);
          yield* assertSameLink(caller, target);
        }
      }
      // Modes can rise after this check; the orchestrator rechecks under lock and records refusal before handlers wrap errors.
      const refused = yield* Ref.make<DispatchModeRefusal | undefined>(undefined);
      return yield* McpRouting.invoke(params, handle).pipe(
        Effect.provideService(DispatchModeLimit, { ...caller.limits, refused }),
        Effect.catch((error) =>
          Effect.flatMap(Ref.get(refused), (refusal) =>
            Effect.fail<E | OrchestratorMcpFailure>(
              refusal === undefined ? error : escalationDenied(refusal),
            ),
          ),
        ),
      );
    }),
  );

const escalationDenied = (refusal: DispatchModeRefusal) =>
  new OrchestratorMcpFailure({
    code:
      refusal.mode === "runtime"
        ? "runtime_mode_escalation_denied"
        : "interaction_mode_escalation_denied",
    message: `Thread ${refusal.threadId} now runs in ${refusal.runtimeMode}/${refusal.interactionMode} mode, above this caller's. Its user changed it while this call ran.`,
  });

export interface StartedModes {
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
  /** The linked environment the caller acts for. The thread must carry it. */
  readonly linkOrigin: OrchestrationV2LinkOrigin | undefined;
}

export const startsThreads = <P, A, E, R>(
  modes: (params: P) => {
    readonly runtimeMode?: RuntimeMode | undefined;
    readonly interactionMode?: ProviderInteractionMode | undefined;
  },
  handle: (params: P, modes: StartedModes) => Effect.Effect<A, E, R>,
  linked: "stamped" | { readonly refused: string },
) =>
  declare((params: P) =>
    Effect.gen(function* () {
      const caller = yield* writingCaller;
      const { limits } = caller;
      if (linked !== "stamped") yield* assertNotLinked(caller, linked.refused);
      const requested = modes(params);
      const started: StartedModes = {
        runtimeMode: yield* resolveRuntimeMode(limits.runtimeMode, requested.runtimeMode),
        interactionMode: yield* resolveInteractionMode(
          limits.interactionMode,
          requested.interactionMode,
        ),
        linkOrigin: callerLinkOrigin(caller),
      };
      return yield* McpRouting.invoke(params, (local) => handle(local, started));
    }),
  );

const fullAccessRequired =
  "Changing projects or environment settings needs a live full-access/default calling thread or a full-access client.";

/** Recheck after waits before writing; caller modes may have changed. */
export const writesEnvironment = <P, A, E, R>(
  handle: (
    params: P,
    check: Effect.Effect<Caller, OrchestratorMcpFailure, CheckServices>,
  ) => Effect.Effect<A, E, R>,
  effect: "owned" | "external-unknown" = "owned",
) => {
  const check = orchestratingCaller.pipe(
    Effect.tap((caller) =>
      McpRouting.CurrentTool.pipe(
        Effect.flatMap((tool) =>
          (tool?.name === "t3_environment_link" || tool?.name === "t3_environment_unlink") &&
          callerLinkOrigin(caller) !== undefined
            ? Effect.fail(
                new OrchestratorMcpFailure({
                  code: "capability_denied",
                  message: "Linked callers cannot change this environment's links.",
                }),
              )
            : Effect.void,
        ),
      ),
    ),
    Effect.tap((caller) => assertNotLinked(caller, "change projects or environment settings")),
    Effect.tap((caller) => assertFullAccess(caller, fullAccessRequired)),
  );
  return declare(
    (params: P) =>
      check.pipe(Effect.flatMap(() => McpRouting.invoke(params, (local) => handle(local, check)))),
    effect,
  );
};

type CheckServices = Effect.Services<typeof orchestratingCaller>;

type Declarations<Handlers> = {
  readonly [Name in keyof Handlers]: Declaration<Handlers[Name]> & NotCallable;
};

type NotCallable = { readonly call?: never } & { readonly apply?: never };

export type Handlers<Tools extends Record<string, Tool.Any>> = Declarations<
  Toolkit.HandlersFrom<Tools>
>;

export class HandlersLayer<Tools extends Record<string, Tool.Any>, EX = never, RX = never> {
  readonly #layer: Layer.Layer<Tool.HandlersFor<Tools>, EX, RX>;
  private constructor(
    token: typeof builtHere,
    layer: Layer.Layer<Tool.HandlersFor<Tools>, EX, RX>,
  ) {
    refuseOutsideConstruction(token);
    this.#layer = layer;
  }
  static {
    handlersLayer = (layer) => new HandlersLayer(builtHere, layer);
  }
  static layer<Tools extends Record<string, Tool.Any>, EX, RX>(
    handlers: HandlersLayer<Tools, EX, RX>,
  ) {
    return handlers.#layer;
  }
}

const checkedHandlers = <Handlers>(declarations: Declarations<Handlers>): Handlers =>
  Struct.map(declarations, checkedHandler);

const routedHandlers = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.Toolkit<Tools>,
  declarations: Handlers<Tools>,
): Toolkit.HandlersFrom<Tools> => {
  const handlers: {
    -readonly [K in keyof Toolkit.HandlersFrom<Tools>]: Toolkit.HandlersFrom<Tools>[K];
  } = {
    ...checkedHandlers<Toolkit.HandlersFrom<Tools>>(declarations),
  };
  for (const name of Object.keys(handlers) as Array<keyof Toolkit.HandlersFrom<Tools>>) {
    const tool = toolkit.tools[name as keyof Tools];
    if (tool === undefined) throw new Error(`Missing registered tool ${String(name)}.`);
    registerToolEffect(tool, declarations[name].effect);
    const handler = handlers[name];
    handlers[name] = (params, context) =>
      handler(params, context).pipe(
        Effect.provideService(McpRouting.CurrentTool, toolkit.tools[name as keyof Tools]),
        Effect.provideService(
          McpRouting.CurrentEffect,
          classifyToolEffect(declarations[name].effect, params),
        ),
      );
  }
  // The nominal declarations were checked against this toolkit before mapping.
  // TypeScript cannot reduce the remapped conditional keys after removing readonly.
  return handlers as unknown as Toolkit.HandlersFrom<Tools>;
};

/** `Toolkit.toLayer` for handlers that all declare their access. */
export const toLayer = <Tools extends Record<string, Tool.Any>, EX = never, RX = never>(
  toolkit: Toolkit.Toolkit<Tools>,
  build: Handlers<Tools> | Effect.Effect<Handlers<Tools>, EX, RX>,
): HandlersLayer<Tools, EX, Exclude<RX, Scope.Scope>> =>
  handlersLayer(
    toolkit.toLayer(
      Effect.isEffect(build)
        ? Effect.map(build, (declarations) => routedHandlers(toolkit, declarations))
        : routedHandlers(toolkit, build),
    ),
  );
