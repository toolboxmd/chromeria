import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  type HostResourcesSnapshot,
  NodeId,
  OrchestratorMcpFailure,
  type OrchestratorMcpDelegateTaskInput,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import { TestClock } from "effect/testing";

import type * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as HostResources from "../resourceTelemetry/HostResources.ts";
import * as Settings from "../serverSettings.ts";
import { git, layerRealGit, pushedCheckouts } from "./DelegatedWorktreeBase.testkit.ts";
import * as DelegationPlacement from "./DelegationPlacement.ts";
import * as PeerForwarding from "./PeerForwarding.ts";
import * as PeerLinks from "./PeerLinks.ts";
import * as RemoteDelegation from "./RemoteDelegation.ts";

const here = EnvironmentId.make("environment-laptop");
const box = EnvironmentId.make("environment-box");
const boxProject = ProjectId.make("project:box");
const parentThreadId = ThreadId.make("thread:parent");
const remoteTaskId = NodeId.make("node:remote-task");

const scope = {
  environmentId: here,
  requestNamespace: "provider-session:parent",
  thread: {
    threadId: parentThreadId,
    providerSessionId: "provider-session:parent",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
} as McpInvocationContext.McpThreadInvocationScope;

const load = (cpuUtilization: number): HostResourcesSnapshot => ({
  sampledAt: 0,
  cpuUtilization,
  cpuCount: 8,
  availableMemoryBytes: 8 * 1024 ** 3,
  totalMemoryBytes: 16 * 1024 ** 3,
});

const task: OrchestratorMcpDelegateTaskInput = {
  task: "Fix the bug.",
  isolation: "worktree",
  clientRequestId: "fix-bug",
};

interface Box {
  readonly reachable: "reachable" | "unreachable";
  readonly access: "read-only" | "approval-required" | "full-access";
  /** This machine's Auto balance weight for the box. */
  readonly weight: number;
  /** Older machines report no load. */
  readonly reportsLoad: boolean;
  readonly load: number;
  readonly hasProject: boolean;
  /** Never answers its environment read. */
  readonly slow: boolean;
  readonly launch:
    | "accepted"
    | "refused before recording"
    | "refused after recording"
    | "refused, records unreadable";
  /** Clone attempts there that fail before one succeeds. */
  readonly cloneFailures: number;
}

const healthyBox: Box = {
  reachable: "reachable",
  access: "full-access",
  weight: 50,
  reportsLoad: true,
  load: 0.1,
  hasProject: true,
  slow: false,
  launch: "accepted",
  cloneFailures: 0,
};

const harness = (options: {
  readonly box?: Partial<Box>;
  readonly autoBalance?: boolean;
  readonly hereLoad?: number;
  readonly unpushed?: boolean;
  /** This request was already recorded on the box. */
  readonly recordedBefore?: boolean;
}) =>
  Effect.gen(function* () {
    const checkouts = yield* pushedCheckouts;
    if (options.unpushed === true)
      git(checkouts.parent, "commit", "-q", "--allow-empty", "-m", "x");
    const boxState: Box = { ...healthyBox, ...options.box };
    const delegated = yield* Ref.make<ReadonlyArray<OrchestratorMcpDelegateTaskInput>>([]);
    const forwarded = yield* Ref.make<ReadonlyArray<string>>([]);
    const recordOf = (clientRequestId: string | undefined) => ({
      id: remoteTaskId,
      remoteChild: {
        environmentId: box,
        launch: {
          clientRequestId: `delegate:${parentThreadId}:${clientRequestId}`,
          projectId: boxProject,
        },
      },
    });
    const recorded = yield* Ref.make<ReadonlyArray<unknown>>(
      options.recordedBefore === true ? [recordOf(task.clientRequestId)] : [],
    );
    const recordsBroken = yield* Ref.make(false);
    const reading = yield* Deferred.make<void>();
    const cloneAttempts = yield* Queue.unbounded<void>();
    const cloneFailures = yield* Ref.make(boxState.cloneFailures);
    const home = checkouts.parent.slice(0, checkouts.parent.lastIndexOf("/"));
    const layer = DelegationPlacement.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(Settings.ServerSettingsService)({
            getSettings: Effect.succeed({
              ...DEFAULT_SERVER_SETTINGS,
              linkedEnvironmentsEnabled: true,
              loadBalancingEnabled: options.autoBalance ?? true,
              loadBalancingWeights: { [box]: boxState.weight },
            }),
          }),
          Layer.mock(PeerLinks.PeerLinks)({
            list: Effect.succeed([
              {
                environmentId: box,
                label: "Box",
                status: boxState.reachable,
                access: boxState.access,
              } as never,
            ]),
          }),
          Layer.mock(PeerForwarding.PeerForwarding)({
            call: ((_scope: unknown, tool: { name: string }, _environmentId: unknown) =>
              Ref.update(forwarded, (seen) => [...seen, tool.name]).pipe(
                Effect.andThen(() => {
                  switch (tool.name) {
                    case "t3_environment_read":
                      return boxState.slow
                        ? Deferred.succeed(reading, undefined).pipe(Effect.andThen(Effect.never))
                        : Effect.succeed({
                            preferences: {},
                            ...(boxState.reportsLoad ? { hostResources: load(boxState.load) } : {}),
                          });
                    case "t3_project_list":
                      return Effect.succeed({
                        projects: boxState.hasProject
                          ? [
                              {
                                id: boxProject,
                                repositoryIdentity: { canonicalKey: "github.com/acme/app" },
                              },
                            ]
                          : [],
                        nextCursor: null,
                      });
                    case "t3_project_clone":
                      return Queue.offer(cloneAttempts, undefined).pipe(
                        Effect.andThen(Ref.getAndUpdate(cloneFailures, (left) => left - 1)),
                        Effect.flatMap((left) =>
                          left > 0
                            ? Effect.fail(
                                new OrchestratorMcpFailure({
                                  code: "orchestration_error",
                                  message: "Clone failed.",
                                }),
                              )
                            : Effect.succeed({ cwd: "/box/home/app" }),
                        ),
                      );
                    default:
                      return Effect.succeed({});
                  }
                }),
              )) as never,
          }),
          Layer.mock(RemoteDelegation.RemoteDelegation)({
            delegate: (_scope, input) =>
              Ref.update(delegated, (seen) => [...seen, input]).pipe(
                Effect.andThen(
                  boxState.launch === "accepted"
                    ? Effect.succeed({ taskId: remoteTaskId })
                    : Effect.gen(function* () {
                        if (boxState.launch === "refused after recording")
                          yield* Ref.set(recorded, [recordOf(input.clientRequestId)]);
                        if (boxState.launch === "refused, records unreadable")
                          yield* Ref.set(recordsBroken, true);
                        return yield* new OrchestratorMcpFailure({
                          code: "orchestration_error",
                          message: "The box refused.",
                        });
                      }),
                ),
              ),
          }),
          Layer.mock(ThreadManagement.ThreadManagementService)({
            // The store's own error type is irrelevant here: any read failure must fail closed.
            getThreadRecords: (() =>
              Ref.get(recordsBroken).pipe(
                Effect.flatMap((broken) =>
                  broken
                    ? Effect.fail(
                        new OrchestratorMcpFailure({
                          code: "orchestration_error",
                          message: "The database is busy.",
                        }),
                      )
                    : Ref.get(recorded),
                ),
                Effect.map(
                  (subagents) =>
                    ({
                      thread: {
                        projectId: ProjectId.make("project:laptop"),
                        worktreePath: null,
                        runtimeMode: "full-access",
                        interactionMode: "default",
                      },
                      runs: [],
                      subagents,
                    }) as never,
                ),
              )) as never,
          }),
          Layer.mock(ProjectService.ProjectService)({
            getById: () => Effect.succeedSome({ workspaceRoot: checkouts.parent } as never),
          }),
          Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
            resolve: () => Effect.succeed({ canonicalKey: "github.com/acme/app" } as never),
          }),
          Layer.mock(HostResources.HostResources)({
            read: Effect.succeed(load(options.hereLoad ?? 0.9)),
          }),
          layerRealGit,
          NodeServices.layer,
          NodeCrypto.layer,
          Layer.succeed(HostProcess.HomeDirectory, home),
        ),
      ),
    );
    const placement = Context.get(
      yield* Layer.build(layer),
      DelegationPlacement.DelegationPlacement,
    );
    return { placement, delegated, forwarded, reading, cloneAttempts };
  });

it.effect.each([
  ["a busier parent machine sends the task to the box", {}, remoteTaskId],
  ["Auto balance off keeps it here", { autoBalance: false }, undefined],
  ["a Manual only box never receives it", { box: { weight: 0 } }, undefined],
  ["a box too old to report its load is skipped", { box: { reportsLoad: false } }, undefined],
  ["a read-only link is not eligible", { box: { access: "read-only" } }, undefined],
  [
    "a link narrower than the parent's modes is not eligible",
    { box: { access: "approval-required" } },
    undefined,
  ],
  ["an unreachable box is skipped", { box: { reachable: "unreachable" } }, undefined],
  ["a less loaded parent machine keeps it", { hereLoad: 0.05, box: { load: 0.8 } }, undefined],
  ["an unpushed base keeps it here", { unpushed: true }, undefined],
  [
    "a box refusing before recording leaves it to run here",
    { box: { launch: "refused before recording" } },
    undefined,
  ],
] as const)("%s", ([, options, expected]) =>
  Effect.gen(function* () {
    const { placement, delegated } = yield* harness(options as Parameters<typeof harness>[0]);
    expect(yield* placement.delegate(scope, task, false)).toBe(expected);
    const sent = yield* Ref.get(delegated);
    if (expected === remoteTaskId) {
      expect(sent).toHaveLength(1);
      expect(sent[0]!.target).toEqual({ environmentId: box, projectId: boxProject });
      expect(sent[0]!.clientRequestId).toBe("fix-bug");
      expect(sent[0]!.task).toContain("You run on Box");
    }
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("a slow box is skipped instead of failing the delegation", () =>
  Effect.gen(function* () {
    const { placement, delegated, reading } = yield* harness({ box: { slow: true } });
    const fiber = yield* placement.delegate(scope, task, false).pipe(Effect.forkChild);
    yield* Deferred.await(reading);
    yield* TestClock.adjust("3 seconds");
    expect(yield* Fiber.join(fiber)).toBeUndefined();
    expect(yield* Ref.get(delegated)).toEqual([]);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("only worktree tasks without a named machine or project are placed", () =>
  Effect.gen(function* () {
    const { placement, forwarded } = yield* harness({});
    for (const input of [
      { ...task, isolation: "shared" as const },
      { ...task, target: { environmentId: here } },
      { ...task, projectId: ProjectId.make("project:other") },
      { ...task, workspaceStrategy: { type: "root" as const } },
    ])
      expect(yield* placement.delegate(scope, input, false)).toBeUndefined();
    // A request already accepted here stays here on retry.
    expect(yield* placement.delegate(scope, task, true)).toBeUndefined();
    expect(yield* Ref.get(forwarded)).toEqual([]);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("a box without the project starts a background clone and the task stays here", () =>
  Effect.gen(function* () {
    const { placement, delegated, cloneAttempts } = yield* harness({
      box: { hasProject: false, cloneFailures: 1 },
    });
    expect(yield* placement.delegate(scope, task, false)).toBeUndefined();
    yield* Queue.take(cloneAttempts);
    // That clone failed, so the next child tries again instead of waiting for a restart.
    expect(
      yield* placement.delegate(scope, { ...task, clientRequestId: "next" }, false),
    ).toBeUndefined();
    yield* Queue.take(cloneAttempts);
    expect(yield* Ref.get(delegated)).toEqual([]);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("a retry lands on the machine its first request was recorded on", () =>
  Effect.gen(function* () {
    const { placement, delegated } = yield* harness({
      box: { launch: "refused after recording" },
    });
    // Recorded there, then refused: the failure is the task's, never a second local child.
    const failed = yield* Effect.flip(placement.delegate(scope, task, false));
    expect(failed.message).toBe("The box refused.");
    const retried = yield* Effect.flip(placement.delegate(scope, task, false));
    expect(retried.message).toBe("The box refused.");
    expect((yield* Ref.get(delegated)).map((input) => input.target?.environmentId)).toEqual([
      box,
      box,
    ]);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("a request recorded there replays there even after Auto balance is turned off", () =>
  Effect.gen(function* () {
    const { placement, delegated } = yield* harness({ autoBalance: false, recordedBefore: true });
    expect(yield* placement.delegate(scope, task, false)).toBe(remoteTaskId);
    expect((yield* Ref.get(delegated)).map((input) => input.target?.environmentId)).toEqual([box]);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("a refusal whose record cannot be read fails instead of running here", () =>
  Effect.gen(function* () {
    const { placement } = yield* harness({ box: { launch: "refused, records unreadable" } });
    const failed = yield* Effect.flip(placement.delegate(scope, task, false));
    expect(failed.message).toBe("The box refused.");
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
