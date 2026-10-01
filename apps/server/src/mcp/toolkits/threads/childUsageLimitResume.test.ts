// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  CommandId,
  EnvironmentId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type OrchestrationEvent,
  type ServerProvider,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import type { Tool } from "effect/unstable/ai";
import { describe, expect, it } from "@effect/vitest";

import { ServerConfig } from "../../../config.ts";
import { OrchestrationEngineLive } from "../../../orchestration/Layers/OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "../../../orchestration/Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "../../../orchestration/Layers/ProjectionSnapshotQuery.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "../../../orchestration/ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../../../orchestration/ThreadPlanProgress.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../../../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../../../persistence/Layers/OrchestrationEventStore.ts";
import { makeSqlitePersistenceLive } from "../../../persistence/Layers/Sqlite.ts";
import * as RepositoryIdentityResolver from "../../../project/RepositoryIdentityResolver.ts";
import * as ProviderRegistry from "../../../provider/Services/ProviderRegistry.ts";
import * as ProviderService from "../../../provider/Services/ProviderService.ts";
import { ServerSettingsService } from "../../../serverSettings.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { ThreadsToolkitHandlersLive } from "./handlers.ts";
import { makeSubagentThreadId } from "./subagentThreadId.ts";
import { ThreadsToolkit } from "./tools.ts";
import { RESUME_TEXT } from "./usageLimitResume.ts";

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));
const INSTANCE = ProviderInstanceId.make("claudeAgent");
const PROJECT_ID = ProjectId.make("project-limit");
const PARENT_ID = ThreadId.make("parent-limit");
const JOB_ID = ThreadId.make(makeSubagentThreadId(PARENT_ID, "f03bcdab"));
const LIMIT_ERROR = "Claude usage limit reached. Send the message again once the limit resets.";

let commandCount = 0;
const commandId = () => CommandId.make(`test-limit-${++commandCount}`);

// The Usage panel's reading when the turns fail: the session window is spent
// and claims to reset three hours later.
const limitedProvider = {
  instanceId: INSTANCE,
  usageLimits: {
    checkedAt: iso(0),
    windows: [
      {
        id: "session",
        kind: "session",
        label: "Session",
        usedPercent: 100,
        resetsAt: iso(3 * HOUR),
      },
    ],
  },
} as unknown as ServerProvider;

// The same reading once the session window has reset.
const liftedProvider = {
  instanceId: INSTANCE,
  usageLimits: {
    checkedAt: iso(3 * HOUR),
    windows: [
      {
        id: "session",
        kind: "session",
        label: "Session",
        usedPercent: 0,
        resetsAt: iso(8 * HOUR),
      },
    ],
  },
} as unknown as ServerProvider;

/** One server process: real engine and projections over a SQLite file, plus the toolkit. */
const serverLayer = (databasePath: string) => {
  const orchestration = Layer.mergeAll(
    OrchestrationEngineLive.pipe(
      Layer.provide(OrchestrationProjectionSnapshotQueryLive),
      Layer.provide(OrchestrationProjectionPipelineLive),
    ),
    OrchestrationProjectionSnapshotQueryLive,
  ).pipe(
    Layer.provideMerge(ThreadBackgroundLiveness.layer),
    Layer.provide(ThreadPlanProgress.layer),
    Layer.provide(OrchestrationEventStoreLive),
    Layer.provideMerge(OrchestrationCommandReceiptRepositoryLive),
    Layer.provide(RepositoryIdentityResolver.layer),
    Layer.provide(makeSqlitePersistenceLive(databasePath)),
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-child-limit-" })),
    Layer.provideMerge(NodeServices.layer),
  );
  const dependencies = Layer.mergeAll(
    orchestration,
    ServerSettingsService.layerTest(),
    Layer.mock(ProviderService.ProviderService)({
      getInstanceInfo: (instanceId) =>
        Effect.succeed({
          instanceId,
          driverKind: "claudeAgent",
          displayName: undefined,
          enabled: true,
        } as never),
    }),
    Layer.mock(ProviderRegistry.ProviderRegistry)({
      getProviders: Effect.map(Clock.currentTimeMillis, (now) => [
        now >= 3 * HOUR ? liftedProvider : limitedProvider,
      ]),
    }),
  );
  return Layer.mergeAll(ThreadsToolkitHandlersLive.pipe(Layer.provide(dependencies)), dependencies);
};

type ThreadTools = typeof ThreadsToolkit.tools;

/** Calls one thread tool as the parent and returns its result. */
const callTool = <Name extends keyof ThreadTools>(
  name: Name,
  params: Tool.Parameters<ThreadTools[Name]>,
) =>
  Effect.gen(function* () {
    const toolkit = yield* ThreadsToolkit;
    return yield* toolkit.handle(name, params as never).pipe(
      Stream.unwrap,
      Stream.runCollect,
      Effect.map((chunk) => chunk.at(-1)!.result as Tool.Success<ThreadTools[Name]>),
      Effect.provideService(McpInvocationContext.McpInvocationContext, {
        environmentId: EnvironmentId.make("environment-limit"),
        threadId: PARENT_ID,
        providerSessionId: "provider-session-limit",
        providerInstanceId: INSTANCE,
        capabilities: new Set<McpInvocationContext.McpCapability>(),
        issuedAt: 1,
      }),
    );
  });

const spawnDirectChild = Effect.gen(function* () {
  const toolkit = yield* ThreadsToolkit;
  return yield* toolkit.handle("spawn_thread", { task: "Implement the Issue." }).pipe(
    Stream.unwrap,
    Stream.runCollect,
    Effect.map(
      (chunk) =>
        chunk.at(-1)!.result as Tool.Success<(typeof ThreadsToolkit.tools)["spawn_thread"]>,
    ),
    Effect.provideService(McpInvocationContext.McpInvocationContext, {
      environmentId: EnvironmentId.make("environment-limit"),
      threadId: PARENT_ID,
      providerSessionId: "provider-session-limit",
      providerInstanceId: INSTANCE,
      capabilities: new Set<McpInvocationContext.McpCapability>(),
      issuedAt: 1,
    }),
  );
});

/** Runs `act`, then waits for the first domain event matching `until`. */
const dispatchUntil = <A, E, R>(
  act: Effect.Effect<A, E, R>,
  until: (event: OrchestrationEvent) => boolean,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const engine = yield* OrchestrationEngineService;
      const events = yield* engine.subscribeDomainEvents;
      const result = yield* act;
      yield* events.pipe(Stream.filter(until), Stream.runHead);
      return result;
    }),
  );

const dispatchAll = (
  commands: ReadonlyArray<Parameters<OrchestrationEngineService["Service"]["dispatch"]>[0]>,
) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    for (const command of commands) yield* engine.dispatch(command);
  });

const session = (
  threadId: ThreadId,
  status: "running" | "ready" | "error",
  turnId: string | null,
  lastError: string | null = null,
) =>
  ({
    type: "thread.session.set",
    commandId: commandId(),
    threadId,
    session: {
      threadId,
      status,
      providerName: "claudeAgent",
      providerInstanceId: INSTANCE,
      runtimeMode: "full-access",
      activeTurnId: turnId === null ? null : TurnId.make(turnId),
      lastError,
      updatedAt: iso(0),
    },
    createdAt: iso(0),
  }) as const;

/** A turn that starts and fails on the usage limit. */
const failOnLimit = (threadId: ThreadId, turnId: string) => [
  session(threadId, "running", turnId),
  session(threadId, "error", turnId, LIMIT_ERROR),
];

const parentFailedRow = (childId: string) => (event: OrchestrationEvent) =>
  event.type === "thread.activity-appended" &&
  event.aggregateId === PARENT_ID &&
  (event.payload.activity.payload as { taskId?: string; status?: string }).taskId === childId &&
  (event.payload.activity.payload as { status?: string }).status === "failed";

const userMessages = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const snapshots = yield* ProjectionSnapshotQuery;
    return Option.getOrThrow(yield* snapshots.getThreadDetailById(threadId))
      .messages.filter((message) => message.role === "user")
      .map((message) => message.text);
  });

describe("usage-limit resume for child threads (toolboxmd/chromeria#71)", () => {
  it.effect(
    "resumes a direct child at the first reply on its instance, tells the parent once, and leaves Prism jobs alone",
    () =>
      Effect.gen(function* () {
        const directory = yield* Effect.promise(() =>
          NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-child-limit-")),
        );
        yield* Effect.addFinalizer(() =>
          Effect.promise(() => NodeFSP.rm(directory, { recursive: true, force: true })),
        );
        yield* Effect.gen(function* () {
          yield* dispatchAll([
            {
              type: "project.create",
              commandId: commandId(),
              projectId: PROJECT_ID,
              title: "Limit",
              workspaceRoot: directory,
              createdAt: iso(0),
            },
            {
              type: "thread.create",
              commandId: commandId(),
              threadId: PARENT_ID,
              projectId: PROJECT_ID,
              title: "Parent",
              modelSelection: { instanceId: INSTANCE, model: "claude-opus-5-5" },
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: null,
              createdAt: iso(0),
            },
          ]);
          const { threadId } = yield* spawnDirectChild;
          const child = ThreadId.make(threadId);
          // A Prism job thread, opened the way Model Router opens them.
          yield* dispatchAll([
            {
              type: "thread.create",
              commandId: commandId(),
              threadId: JOB_ID,
              projectId: PROJECT_ID,
              title: "Retry Release Reconciliation Dispatch",
              modelSelection: { instanceId: INSTANCE, model: "claude-opus-5-5" },
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: null,
              createdAt: iso(0),
            },
            {
              type: "thread.turn.start",
              commandId: commandId(),
              threadId: JOB_ID,
              message: {
                messageId: MessageId.make("job-task"),
                role: "user",
                text: `[model-router job prism-1 worker seq 1 on route t3:claudeAgent:claude-opus-5-5@medium; planner thread ${PARENT_ID}]\n\nDo it.`,
                attachments: [],
              },
              runtimeMode: "full-access",
              interactionMode: "default",
              createdAt: iso(0),
            },
          ]);

          // Both hit the limit. The toolkit handles events in order, so once
          // the job's failure row lands, the child's resume is scheduled.
          yield* dispatchAll(failOnLimit(child, "child-turn-1"));
          yield* dispatchUntil(
            dispatchAll(failOnLimit(JOB_ID, "job-turn-1")),
            parentFailedRow(JOB_ID),
          );

          // Past the settle delay, the parent gets a reply on the same instance.
          yield* TestClock.adjust(MINUTE);
          yield* dispatchUntil(
            dispatchAll([
              session(PARENT_ID, "running", "parent-turn-1"),
              {
                type: "thread.message.assistant.delta",
                commandId: commandId(),
                threadId: PARENT_ID,
                messageId: MessageId.make("parent-reply-1"),
                delta: "Still working.",
                createdAt: iso(MINUTE),
              },
              {
                type: "thread.message.assistant.complete",
                commandId: commandId(),
                threadId: PARENT_ID,
                messageId: MessageId.make("parent-reply-1"),
                createdAt: iso(MINUTE),
              },
            ]),
            (event) =>
              event.type === "thread.message-sent" &&
              event.aggregateId === PARENT_ID &&
              event.payload.text.includes("resumed automatically"),
          );

          // Long after the displayed reset nothing else was sent.
          yield* TestClock.adjust(4 * HOUR);
          yield* dispatchUntil(
            dispatchAll([session(child, "running", "child-turn-2")]),
            (event) =>
              event.type === "thread.activity-appended" &&
              event.aggregateId === PARENT_ID &&
              (event.payload.activity.payload as { status?: string }).status === "running",
          );

          expect((yield* userMessages(child)).slice(1)).toEqual([RESUME_TEXT]);
          expect((yield* userMessages(JOB_ID)).slice(1)).toEqual([]);
          expect(
            (yield* userMessages(PARENT_ID)).filter((text) => text.includes("resumed")),
          ).toEqual([
            `[Subagent Subagent: Implement the Issue. (thread ${threadId}) resumed automatically: another thread got a reply from the same provider, so its usage limit has lifted.]`,
          ]);
        }).pipe(Effect.provide(serverLayer(NodePath.join(directory, "state.sqlite"))));
      }).pipe(Effect.scoped),
  );
});

/** The project and parent every test spawns from. */
const createParent = (directory: string) =>
  dispatchAll([
    {
      type: "project.create",
      commandId: commandId(),
      projectId: PROJECT_ID,
      title: "Limit",
      workspaceRoot: directory,
      createdAt: iso(0),
    },
    {
      type: "thread.create",
      commandId: commandId(),
      threadId: PARENT_ID,
      projectId: PROJECT_ID,
      title: "Parent",
      modelSelection: { instanceId: INSTANCE, model: "claude-opus-5-5" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdAt: iso(0),
    },
  ]);

/** A finished assistant reply on `threadId`. */
const reply = (threadId: ThreadId, messageId: string, text: string, at: number) => [
  {
    type: "thread.message.assistant.delta",
    commandId: commandId(),
    threadId,
    messageId: MessageId.make(messageId),
    delta: text,
    createdAt: iso(at),
  } as const,
  {
    type: "thread.message.assistant.complete",
    commandId: commandId(),
    threadId,
    messageId: MessageId.make(messageId),
    createdAt: iso(at),
  } as const,
];

const parentRow = (childId: string, status: string) => (event: OrchestrationEvent) =>
  event.type === "thread.activity-appended" &&
  event.aggregateId === PARENT_ID &&
  (event.payload.activity.payload as { taskId?: string }).taskId === childId &&
  (event.payload.activity.payload as { status?: string }).status === status;

const temporaryDirectory = Effect.gen(function* () {
  const directory = yield* Effect.promise(() =>
    NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-child-limit-")),
  );
  yield* Effect.addFinalizer(() =>
    Effect.promise(() => NodeFSP.rm(directory, { recursive: true, force: true })),
  );
  return directory;
});

describe("usage-limit resume with interrupt_thread and report-back", () => {
  it.effect("never continues a limit-hit child that interrupt_thread stopped", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory;
      yield* Effect.gen(function* () {
        yield* createParent(directory);
        const child = ThreadId.make((yield* spawnDirectChild).threadId);
        const marker = ThreadId.make((yield* spawnDirectChild).threadId);
        // The bridge handles events in order: once the marker's running row
        // lands, the child's resume is scheduled.
        yield* dispatchAll(failOnLimit(child, "child-turn-1"));
        yield* dispatchUntil(
          dispatchAll([session(marker, "running", "marker-turn-1")]),
          parentRow(marker, "running"),
        );

        // The dispatcher replaces the child: it interrupts it first.
        expect(yield* callTool("interrupt_thread", { threadId: child, scope: "children" })).toEqual(
          { threadId: child, turnId: null, status: "no_active_run", statusAfter: "failed" },
        );

        // A reply on the instance, then the displayed reset passes.
        yield* TestClock.adjust(MINUTE);
        yield* dispatchAll([
          session(PARENT_ID, "running", "parent-turn-1"),
          ...reply(PARENT_ID, "parent-reply-1", "Still working.", MINUTE),
        ]);
        yield* TestClock.adjust(4 * HOUR);
        yield* dispatchUntil(
          dispatchAll([session(marker, "ready", null)]),
          parentRow(marker, "idle"),
        );

        expect((yield* userMessages(child)).slice(1)).toEqual([]);
        expect((yield* userMessages(PARENT_ID)).filter((text) => text.includes("resumed"))).toEqual(
          [],
        );
      }).pipe(Effect.provide(serverLayer(NodePath.join(directory, "state.sqlite"))));
    }).pipe(Effect.scoped),
  );

  it.effect("reports a continued child's reply once, also after a restart", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory;
      const databasePath = NodePath.join(directory, "state.sqlite");
      const reports = (messages: ReadonlyArray<string>) =>
        messages.filter((text) => text.includes("finished a turn"));
      const childId = yield* Effect.gen(function* () {
        yield* createParent(directory);
        const child = ThreadId.make((yield* spawnDirectChild).threadId);
        // The first turn writes part of a reply, then hits the limit.
        yield* dispatchAll([
          session(child, "running", "child-turn-1"),
          ...reply(child, "child-part-1", "PARTIAL", 0),
        ]);
        yield* dispatchUntil(
          dispatchAll([session(child, "error", "child-turn-1", LIMIT_ERROR)]),
          parentRow(child, "failed"),
        );
        // The displayed reset passes; the child is continued and finishes.
        yield* dispatchUntil(
          TestClock.adjust(4 * HOUR),
          (event) =>
            event.type === "thread.message-sent" &&
            event.aggregateId === child &&
            event.payload.text === RESUME_TEXT,
        );
        yield* dispatchUntil(
          dispatchAll([
            session(child, "running", "child-turn-2"),
            ...reply(child, "child-reply-2", "FINISHED", 4 * HOUR),
            session(child, "ready", null),
          ]),
          (event) =>
            event.type === "thread.message-sent" &&
            event.aggregateId === PARENT_ID &&
            event.payload.text.includes("finished a turn"),
        );
        expect(reports(yield* userMessages(PARENT_ID))).toEqual([
          expect.stringContaining("FINISHED"),
        ]);
        return child;
      }).pipe(Effect.provide(serverLayer(databasePath)));

      // After a restart, an idle transition without a new reply sends nothing.
      const messages = yield* Effect.gen(function* () {
        yield* dispatchUntil(
          dispatchAll([session(childId, "running", "settle"), session(childId, "ready", null)]),
          parentRow(childId, "idle"),
        );
        yield* dispatchUntil(
          dispatchAll([session(childId, "running", "after-settle")]),
          parentRow(childId, "running"),
        );
        return yield* userMessages(PARENT_ID);
      }).pipe(Effect.provide(serverLayer(databasePath)));
      expect(reports(messages)).toEqual([expect.stringContaining("FINISHED")]);
      expect(messages.filter((text) => text.includes("resumed automatically"))).toHaveLength(1);
    }).pipe(Effect.scoped),
  );

  it.effect(
    "never continues a turn whose error is not a usage limit, even while a window is exhausted",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory;
        yield* Effect.gen(function* () {
          yield* createParent(directory);
          const child = ThreadId.make((yield* spawnDirectChild).threadId);
          const marker = ThreadId.make((yield* spawnDirectChild).threadId);
          // The provider snapshot still shows the session window exhausted.
          yield* dispatchAll([
            session(child, "running", "child-turn-1"),
            session(child, "error", "child-turn-1", "Provider process exited with code 1."),
          ]);
          yield* dispatchUntil(
            dispatchAll([session(marker, "running", "marker-turn-1")]),
            parentRow(marker, "running"),
          );
          yield* TestClock.adjust(MINUTE);
          yield* dispatchAll([
            session(PARENT_ID, "running", "parent-turn-1"),
            ...reply(PARENT_ID, "parent-reply-1", "Still working.", MINUTE),
          ]);
          yield* TestClock.adjust(4 * HOUR);
          yield* dispatchUntil(
            dispatchAll([session(marker, "ready", null)]),
            parentRow(marker, "idle"),
          );
          expect((yield* userMessages(child)).slice(1)).toEqual([]);
          expect(
            (yield* userMessages(PARENT_ID)).filter((text) => text.includes("resumed")),
          ).toEqual([]);
        }).pipe(Effect.provide(serverLayer(NodePath.join(directory, "state.sqlite"))));
      }).pipe(Effect.scoped),
  );
});
