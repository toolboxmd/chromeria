import { assert, it } from "@effect/vitest";
import {
  EnvironmentId,
  NodeId,
  NonNegativeInt,
  ProviderInstanceId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";
import { McpSchema, McpServer } from "effect/ai";

import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import {
  McpInvocationContext,
  type McpClientCaller,
  type McpInvocationScope,
} from "../mcp/McpInvocationContext.ts";
import { liveThreadShell, idleThreadProjection } from "../mcp/McpToolAccess.testkit.ts";
import { layer as handlers } from "../mcp/toolkits/thread/handlers.ts";
import { ThreadToolkit } from "../mcp/toolkits/thread/tools.ts";
import { ThreadManagementService } from "../orchestration-v2/ThreadManagementService.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";

const root = ThreadId.make("root");
const child = ThreadId.make("child");
const grandchild = ThreadId.make("grandchild");
const unrelated = ThreadId.make("unrelated");
const fork = ThreadId.make("fork");
const promachos = ThreadId.make("promachos");
const topLevel = ThreadId.make("top-level");
const requestId = RuntimeRequestId.make("request");
const now = DateTime.makeUnsafe("2026-01-01T00:00:00.000Z");
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "requests", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "requests", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const scope = (id: ThreadId | undefined): McpInvocationScope => ({
  environmentId: EnvironmentId.make("env"),
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "request",
  thread:
    id === undefined
      ? undefined
      : {
          threadId: id,
          providerSessionId: "session",
          providerInstanceId: ProviderInstanceId.make("codex"),
        },
  client:
    id === undefined ? { sessionId: "oauth", label: "external", access: "full-access" } : undefined,
});

type Kind = "approval" | "question";

const subagentOf = (parent: ThreadId) => ({
  rootThreadId: root,
  parentThreadId: parent,
  relationshipToParent: "subagent" as const,
});

/** root -> child -> grandchild, a fork of root, and unrelated top-level threads. */
const defaultShells = () =>
  new Map<ThreadId, OrchestrationV2ThreadShell>(
    [root, child, grandchild, unrelated, fork, promachos, topLevel].map((id) => [
      id,
      {
        ...liveThreadShell(id, {
          runtimeMode: id === root || id === promachos ? "full-access" : "approval-required",
        }),
        ...(id === promachos ? { prismRole: "promachos" as const } : {}),
        lineage:
          id === child
            ? subagentOf(root)
            : id === grandchild
              ? subagentOf(child)
              : id === fork
                ? {
                    rootThreadId: root,
                    parentThreadId: root,
                    relationshipToParent: "fork" as const,
                  }
                : { rootThreadId: id, parentThreadId: null, relationshipToParent: null },
      },
    ]),
  );

const respond = (input: {
  readonly scope: McpInvocationScope;
  readonly target: ThreadId;
  readonly kind: Kind;
  readonly shells?: Map<ThreadId, OrchestrationV2ThreadShell>;
}) =>
  Effect.gen(function* () {
    const { target, kind } = input;
    const shells = input.shells ?? defaultShells();
    const commands = yield* Ref.make<ReadonlyArray<OrchestrationV2ServerCommand>>([]);
    const threads = Layer.mock(ThreadManagementService)({
      getThreadShell: (id) => Effect.succeed(shells.get(id) ?? null),
      getProjectThreadRecords: (({ threadId }) => {
        const shell = shells.get(threadId)!;
        const base = {
          id: TurnItemId.make("item"),
          threadId,
          runId: null,
          nodeId: null,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: NonNegativeInt.make(0),
          status: "in_progress" as const,
          title: null,
          startedAt: now,
          completedAt: null,
          updatedAt: now,
        };
        return Effect.succeed({
          ...idleThreadProjection(shell),
          runtimeRequests: [
            {
              id: requestId,
              nodeId: NodeId.make("node"),
              providerTurnId: null,
              nativeRequestRef: null,
              kind: kind === "approval" ? ("command" as const) : ("user_input" as const),
              status: "pending" as const,
              responseCapability: { type: "message" as const },
              createdAt: now,
              resolvedAt: null,
            },
          ],
          turnItems: [
            kind === "approval"
              ? {
                  ...base,
                  type: "approval_request" as const,
                  requestId,
                  requestKind: "command" as const,
                  prompt: "Run command?",
                }
              : { ...base, type: "user_input_request" as const, requestId, questions: [] },
          ],
        });
      }) as ThreadManagementService["Service"]["getProjectThreadRecords"],
      dispatch: (command) =>
        Ref.update(commands, (current) => [...current, command]).pipe(
          Effect.as({ sequence: NonNegativeInt.make(1), storedEvents: [] }),
        ),
    });
    const server = McpHttpServer.toolkitRegistration(ThreadToolkit, handlers).pipe(
      Layer.provideMerge(McpServer.McpServer.layer),
      Layer.provideMerge(threads),
      Layer.provide(Layer.mock(ThreadSearch.ThreadSearch)({})),
      Layer.provide(Layer.mock(ScheduledTasks.ScheduledTaskService)({})),
      Layer.provideMerge(NodeServices.layer),
    );
    const result = yield* McpServer.McpServer.pipe(
      Effect.flatMap((service) =>
        service.callTool({
          name: "t3_pending_request_respond",
          arguments: {
            threadId: target,
            requestId,
            ...(kind === "approval" ? { decision: "accept" } : { answers: { choice: "continue" } }),
          },
        }),
      ),
      Effect.provideService(McpInvocationContext, input.scope),
      Effect.provideService(McpSchema.McpServerClient, client),
      Effect.provide(server),
    );
    const dispatched = yield* Ref.get(commands);
    assert.equal(result.isError === true, dispatched.length === 0);
    assert.isAtMost(dispatched.length, 1);
    const command = dispatched[0];
    return command?.type === "runtime-request.respond" ? command : undefined;
  }).pipe(Effect.scoped);

it.effect.each([
  [root, grandchild, "approval", true],
  [root, grandchild, "question", true],
  [unrelated, grandchild, "approval", false],
  [unrelated, grandchild, "question", false],
  [undefined, grandchild, "approval", false],
  [root, fork, "approval", false],
  [grandchild, grandchild, "approval", false],
  // A thread in another Prism role keeps ancestor-only answering (toolboxmd/chromeria#532).
  [unrelated, topLevel, "question", false],
] as const)("respond authorizes %s -> %s %s", ([caller, target, kind, accepted]) =>
  Effect.gen(function* () {
    const command = yield* respond({ scope: scope(caller), target, kind });
    assert.equal(command !== undefined, accepted);
    assert.isUndefined(command?.answeredBy);
  }),
);

// Fork: the Promachos answers for the user when no ancestor can (toolboxmd/chromeria#532).
const clockAt = (iso: string) =>
  TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe(iso)));
const STALE_NOW = "2026-01-01T01:00:00.000Z";
const withParent = (patch: Partial<OrchestrationV2ThreadShell> | null) => {
  const shells = defaultShells();
  if (patch === null) shells.delete(child);
  else shells.set(child, { ...shells.get(child)!, ...patch });
  // The root is idle and stale, so only the parent can hold first say.
  shells.set(root, { ...shells.get(root)!, activeRunId: null, status: "idle" });
  return shells;
};
const answeredByPromachos = { kind: "promachos", threadId: promachos } as const;

it.effect.each(["question", "approval"] as const)(
  "the Promachos answers a top-level thread's %s on the user's behalf",
  (kind) =>
    Effect.gen(function* () {
      const command = yield* respond({ scope: scope(promachos), target: topLevel, kind });
      assert.deepStrictEqual(command?.answeredBy, answeredByPromachos);
    }),
);

it.effect.each([
  ["running", {}, false],
  [
    "recently active",
    { activeRunId: null, updatedAt: DateTime.makeUnsafe("2026-01-01T00:45:00.000Z") },
    false,
  ],
  ["stale", { activeRunId: null, status: "idle" as const }, true],
  ["deleted", { deletedAt: DateTime.makeUnsafe("2026-01-01T00:10:00.000Z") }, true],
  ["gone", null, true],
] as const)("the Promachos answers a child whose parent is %s: %s", ([_state, patch, accepted]) =>
  Effect.gen(function* () {
    yield* clockAt(STALE_NOW);
    const shells = withParent(patch);
    const command = yield* respond({
      scope: scope(promachos),
      target: grandchild,
      kind: "question",
      shells,
    });
    assert.deepStrictEqual(command?.answeredBy, accepted ? answeredByPromachos : undefined);
    assert.equal(command !== undefined, accepted);
  }),
);

it.effect("an active grandparent keeps first say over a stale parent", () =>
  Effect.gen(function* () {
    yield* clockAt(STALE_NOW);
    const shells = withParent({ activeRunId: null });
    shells.set(root, { ...shells.get(root)!, activeRunId: defaultShells().get(root)!.activeRunId });
    const command = yield* respond({
      scope: scope(promachos),
      target: grandchild,
      kind: "question",
      shells,
    });
    assert.isUndefined(command);
  }),
);

it.effect("a subagent child that copied the Promachos role cannot answer", () =>
  Effect.gen(function* () {
    const shells = defaultShells();
    const worker = ThreadId.make("worker");
    shells.set(worker, {
      ...shells.get(promachos)!,
      id: worker,
      lineage: subagentOf(promachos),
    });
    const command = yield* respond({
      scope: scope(worker),
      target: topLevel,
      kind: "question",
      shells,
    });
    assert.isUndefined(command);
  }),
);

it.effect("an active grandparent keeps first say over a deleted parent", () =>
  Effect.gen(function* () {
    yield* clockAt(STALE_NOW);
    const shells = defaultShells();
    shells.set(child, {
      ...shells.get(child)!,
      activeRunId: null,
      deletedAt: DateTime.makeUnsafe("2026-01-01T00:10:00.000Z"),
    });
    const command = yield* respond({
      scope: scope(promachos),
      target: grandchild,
      kind: "question",
      shells,
    });
    assert.isUndefined(command);
  }),
);

it.effect("a child delegated from another environment stays with its remote parent", () =>
  Effect.gen(function* () {
    const shells = defaultShells();
    shells.set(topLevel, {
      ...shells.get(topLevel)!,
      delegatedFrom: {
        environmentId: EnvironmentId.make("other"),
        threadId: root,
        title: "parent",
      },
    });
    const command = yield* respond({
      scope: scope(promachos),
      target: topLevel,
      kind: "question",
      shells,
    });
    assert.isUndefined(command);
  }),
);

const linked = (client: Partial<McpClientCaller>): McpInvocationScope => ({
  ...scope(undefined),
  client: {
    sessionId: "peer",
    label: "cavallo",
    access: "full-access",
    linked: true,
    promachosThreadId: ThreadId.make("remote-promachos"),
    ...client,
  },
});

it.effect.each([
  ["full-access", {}, true],
  ["approval-required", { access: "approval-required" as const }, false],
  ["read-only", { access: "read-only" as const }, false],
  ["no Promachos", { promachosThreadId: undefined }, false],
] as const)("a %s link answers for its Promachos: %s", ([_name, client, accepted]) =>
  Effect.gen(function* () {
    const command = yield* respond({ scope: linked(client), target: topLevel, kind: "question" });
    assert.deepStrictEqual(
      command?.answeredBy,
      accepted
        ? { kind: "promachos", threadId: ThreadId.make("remote-promachos"), via: "cavallo" }
        : undefined,
    );
    assert.equal(command !== undefined, accepted);
  }),
);

it.effect("a full-access link answers an approval for its Promachos", () =>
  Effect.gen(function* () {
    const command = yield* respond({ scope: linked({}), target: topLevel, kind: "approval" });
    assert.equal(command?.answeredBy?.kind, "promachos");
  }),
);

it.effect("a full-access link narrowed below full access keeps its fence", () =>
  Effect.gen(function* () {
    const command = yield* respond({
      scope: linked({ narrowedTo: { runtimeMode: "auto", interactionMode: "default" } }),
      target: topLevel,
      kind: "question",
    });
    assert.isUndefined(command);
  }),
);

it.effect("a linked Promachos is refused while an ancestor is active", () =>
  Effect.gen(function* () {
    const command = yield* respond({ scope: linked({}), target: grandchild, kind: "question" });
    assert.isUndefined(command);
  }),
);
