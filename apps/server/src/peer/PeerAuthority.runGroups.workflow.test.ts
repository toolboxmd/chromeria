import { layerFixtureHostServices } from "./PeerAuthority.workflow.fixture.ts";
vi.mock("effect/Queue", { spy: true });

import { vi } from "vite-plus/test";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  NodeId,
  OrchestratorMcpRunGroupResult,
  ProviderDriverKind,
  ThreadId,
  type OrchestrationV2Subagent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as SqlClient from "effect/sql/SqlClient";
import { descriptorOf, linkTo } from "./PeerLinks.testkit.ts";
import * as RemoteDelegation from "./RemoteDelegation.ts";
import { runGroupWakeDetail } from "../orchestration-v2/RunGroups.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import { OrchestratorToolkit } from "../mcp/toolkits/orchestrator/tools.ts";
import {
  activate,
  caller,
  decodeTaskReceipt,
  finishLifecycleRun,
  instanceId,
  json,
  makeNode,
  nativeCall,
  seedParent,
  type Node,
} from "./PeerAuthority.workflow.fixture.ts";

const decodeGroup = Schema.decodeUnknownEffect(OrchestratorMcpRunGroupResult);

const services = Layer.merge(
  FetchHttpClient.layer,
  Layer.merge(NodeServices.layer, layerFixtureHostServices),
);

/** A at Run 2 for reviewers; B's reviewer kit lists two Colors and its own kit text. */
const pair = Effect.fn("runGroups.pair")(function* (name: string) {
  const b = yield* makeNode(descriptorOf(`${name}-b`, "B", { peerAuthority: 1 }), "disk");
  const a = yield* makeNode(descriptorOf(`${name}-a`, "A", { peerAuthority: 1 }), "disk");
  yield* linkTo(a.links, b, "full-access");
  yield* a.fixtureSettings.updateSettings({
    prismRoles: { reviewer: { run: 2 } },
  } as never);
  yield* b.fixtureSettings.updateSettings({
    prismRoles: {
      reviewer: {
        instructions: "B review kit.",
        models: [
          { instanceId: "codex", model: "gpt-5.4" },
          { instanceId: "codex", model: "gpt-5" },
        ],
      },
    },
  } as never);
  const parentId = ThreadId.make(`${name}-parent`);
  yield* seedParent(a, parentId);
  return { a, b, parentId };
});

const tasksOf = (node: Node, parentId: ThreadId) =>
  node.threads
    .getThreadRecords(parentId, ["subagents"])
    .pipe(Effect.map(({ subagents }) => subagents.filter((task) => task.origin === "app_owned")));

/** The first recorded state of `taskId` that satisfies `predicate`. */
const recorded = (
  node: Node,
  parentId: ThreadId,
  taskId: NodeId,
  predicate: (task: OrchestrationV2Subagent) => boolean,
) =>
  node.orchestrator
    .streamStoredEventsFrom({ threadId: parentId, afterSequence: 0, eventType: "subagent.updated" })
    .pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "subagent.updated" &&
          stored.event.payload.id === taskId &&
          predicate(stored.event.payload),
      ),
      Stream.runHead,
      Effect.flatMap(Effect.fromOption),
      Effect.map((stored) => stored.event.payload as OrchestrationV2Subagent),
    );

/** Distinct committed wake messages carrying any of `taskIds`. */
const committedWakes = (node: Node, parentId: ThreadId, taskIds: ReadonlyArray<NodeId>) =>
  node.orchestrator.getThreadProjection(parentId).pipe(
    Effect.map(
      (parent) =>
        new Set([
          ...parent.runs.flatMap((run) =>
            run.delegatedCompletion?.delivery?.taskIds.some((id) => taskIds.includes(id)) === true
              ? [run.delegatedCompletion.delivery.messageId]
              : [],
          ),
          ...parent.messages
            .filter((message) => {
              if (message.delegatedCompletion?.taskIds.some((id) => taskIds.includes(id)) !== true)
                return false;
              const run = parent.runs.find((candidate) => candidate.id === message.runId);
              return !(run?.status === "cancelled" && run.startedAt === null);
            })
            .map((message) => message.id),
        ]),
    ),
  );

/** Stops the destination recorded: one `peer-task-stop:*` command each. */
const destinationStops = (node: Node) =>
  Context.get(node.database, SqlClient.SqlClient)<{
    readonly count: number;
  }>`SELECT COUNT(*) AS count FROM peer_task_stop_receipts WHERE command_id LIKE 'peer-task-stop:%'`.pipe(
    Effect.map((rows) => rows[0]?.count ?? 0),
  );

it.live("runGroups751: a Run group to a linked environment starts B's Colors and wakes once", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { a, b, parentId } = yield* pair("rg-basic");
      const receipt = yield* nativeCall(a, parentId, "delegate_task", {
        task: "Review the plan.",
        prismRole: "reviewer",
        target: { environmentId: b.environmentId, projectId: b.project.id },
        clientRequestId: "rg-basic",
      });
      expect(receipt.isError, json(receipt.content)).not.toBe(true);
      const group = yield* decodeGroup(receipt.structuredContent);
      expect(group.tasks).toHaveLength(2);
      const members = yield* tasksOf(a, parentId);
      expect(members.map((task) => task.runGroupId)).toEqual([group.groupId, group.groupId]);
      // The kit travels apart from the task, so a destination that ignores it never doubles it.
      expect(members.map((task) => task.prompt)).toEqual(["Review the plan.", "Review the plan."]);
      const children = yield* Effect.forEach(members, (task) =>
        recorded(a, parentId, task.id, (current) => current.remoteChild?.threadId !== undefined),
      );
      const models = yield* Effect.forEach(children, (task) =>
        b.threads.getThreadRecords(task.remoteChild!.threadId!, ["messages"]).pipe(
          Effect.map((child) => ({
            model: task.remoteChild!.launch!.modelSelection!.model,
            text: child.messages[0]?.text,
          })),
        ),
      );
      expect(models.map((child) => child.model).toSorted()).toEqual(["gpt-5", "gpt-5.4"]);
      for (const child of models) expect(child.text).toBe("B review kit.\n\nReview the plan.");
      for (const [index, task] of children.entries())
        yield* finishLifecycleRun(b, task.remoteChild!.threadId!, `rg-basic-${index}`);
      for (const task of children)
        yield* recorded(a, parentId, task.id, (current) => current.result !== null);
      const wakes = yield* committedWakes(
        a,
        parentId,
        children.map((task) => task.id),
      );
      expect(wakes.size).toBe(1);
      // The one wake carries every member, and its text lists each one's Color, status and result.
      const parent = yield* a.orchestrator.getThreadProjection(parentId);
      const delivery = parent.runs.flatMap((run) => run.delegatedCompletion?.delivery ?? [])[0];
      expect([...(delivery?.taskIds ?? [])].toSorted()).toEqual(ids(children).toSorted());
      const text = runGroupWakeDetail(parent, group.groupId);
      for (const [index, task] of children.entries())
        expect(text).toContain(
          `[${task.id} · ${task.remoteChild!.launch!.modelSelection!.model}] completed\nrg-basic-${index}`,
        );
      // Both members were recorded by one command.
      const sql = Context.get(a.database, SqlClient.SqlClient);
      const commands = yield* sql<{
        readonly command_id: string;
      }>`SELECT DISTINCT command_id FROM orchestration_events WHERE stream_id=${parentId} AND event_type='subagent.updated' AND json_extract(payload_json,'$.runGroupId')=${group.groupId} AND json_extract(payload_json,'$.remoteChild.launchStarted') IS NULL AND json_extract(payload_json,'$.remoteChild.threadId') IS NULL`;
      expect(commands).toHaveLength(1);
      expect(yield* destinationStops(b)).toBe(0);
    }),
  ).pipe(Effect.provide(services)),
);

/** Delegates a reviewer Run group to B and waits until every member is attached there. */
const startGroup = Effect.fn("runGroups.startGroup")(function* (
  a: Node,
  b: Node,
  parentId: ThreadId,
  key: string,
  extra: Record<string, unknown> = {},
) {
  const receipt = yield* nativeCall(a, parentId, "delegate_task", {
    task: "Review the plan.",
    prismRole: "reviewer",
    target: { environmentId: b.environmentId, projectId: b.project.id },
    clientRequestId: key,
    ...extra,
  });
  expect(receipt.isError, json(receipt.content)).not.toBe(true);
  const group = yield* decodeGroup(receipt.structuredContent);
  const members = yield* Effect.forEach(group.tasks, (task) =>
    recorded(a, parentId, task.taskId, (current) => current.remoteChild?.threadId !== undefined),
  );
  return { groupId: group.groupId, members };
});

const childOf = (task: OrchestrationV2Subagent) => task.remoteChild!.threadId!;
const ids = (members: ReadonlyArray<OrchestrationV2Subagent>) => members.map((task) => task.id);

it.live(
  "runGroups751: an all-remote group cancel stops every member there and gives up the wake",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parentId } = yield* pair("rg-cancel");
        const { groupId, members } = yield* startGroup(a, b, parentId, "rg-cancel");
        const cancel = yield* nativeCall(a, parentId, "task_cancel", {
          taskId: groupId,
          reason: "Enough",
          clientRequestId: "rg-cancel-stop",
        });
        expect(cancel.isError, json(cancel.content)).not.toBe(true);
        // The group cancel records every member's intent in one command, with its reason.
        const sql = Context.get(a.database, SqlClient.SqlClient);
        const intents = yield* sql<{
          readonly command_id: string;
          readonly request_id: string;
        }>`SELECT command_id, json_extract(payload_json,'$.remoteChild.cancelIntent.requestId') AS request_id FROM orchestration_events WHERE stream_id=${parentId} AND event_type='subagent.updated' AND json_extract(payload_json,'$.remoteChild.cancelIntent.groupCancelId') IS NOT NULL AND json_extract(payload_json,'$.remoteChild.stop') IS NULL`;
        expect(new Set(intents.map((row) => row.command_id)).size).toBe(1);
        expect(intents.map((row) => row.request_id).toSorted()).toEqual(
          [0, 1]
            .map(
              (index) =>
                `cancel:${encodeURIComponent(`thread:${a.environmentId}:${parentId}`)}:rg-cancel-stop:${index}`,
            )
            .toSorted(),
        );
        for (const task of members)
          yield* recorded(
            a,
            parentId,
            task.id,
            (current) =>
              current.groupCancel !== undefined &&
              current.remoteChild?.cancelResolution?.outcome === "applied" &&
              current.remoteChild.cancelIntent === undefined,
          );
        // Each member's run there ends; its result stays quiet.
        for (const [index, task] of members.entries())
          yield* finishLifecycleRun(b, childOf(task), `rg-cancel-late-${index}`);
        for (const task of members)
          yield* recorded(a, parentId, task.id, (current) => current.result !== null);
        expect((yield* committedWakes(a, parentId, ids(members))).size).toBe(0);
        expect(yield* destinationStops(b)).toBe(2);
      }),
    ).pipe(Effect.provide(services)),
);

it.live(
  "runGroups751: a member that refuses the group cancel inline keeps running and wakes once",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parentId } = yield* pair("rg-refuse");
        const { groupId, members } = yield* startGroup(a, b, parentId, "rg-refuse");
        // The threads there now run above this parent's modes, so the destination refuses.
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: parentId,
          commandId: CommandId.make("rg-refuse-lower"),
          runtimeMode: "auto",
        });
        const cancel = yield* nativeCall(a, parentId, "task_cancel", {
          taskId: groupId,
          clientRequestId: "rg-refuse-stop",
        });
        expect(cancel.isError).toBe(true);
        expect(json(cancel.content)).toContain("runtime_mode_escalation_denied");
        for (const task of members)
          yield* recorded(
            a,
            parentId,
            task.id,
            (current) =>
              current.remoteChild?.cancelResolution?.outcome === "refused" &&
              current.remoteChild.cancelIntent === undefined &&
              current.groupCancel === undefined,
          );
        for (const [index, task] of members.entries())
          yield* finishLifecycleRun(b, childOf(task), `rg-refuse-${index}`);
        for (const task of members)
          yield* recorded(a, parentId, task.id, (current) => current.result !== null);
        expect((yield* committedWakes(a, parentId, ids(members))).size).toBe(1);
        expect(yield* destinationStops(b)).toBe(0);
      }),
    ).pipe(Effect.provide(services)),
);

it.live(
  "runGroups751: a late refusal after a restart keeps the members running, with one wake",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parentId } = yield* pair("rg-late");
        const { groupId, members } = yield* startGroup(a, b, parentId, "rg-late");
        // Unreachable at cancel: each member's Stop is recorded and stays unanswered.
        yield* a.stopConnections;
        const cancel = yield* nativeCall(a, parentId, "task_cancel", {
          taskId: groupId,
          clientRequestId: "rg-late-stop",
        });
        expect(cancel.isError, json(cancel.content)).not.toBe(true);
        for (const task of members)
          expect(
            (yield* tasksOf(a, parentId)).find((current) => current.id === task.id),
          ).toMatchObject({
            result: null,
            remoteChild: { cancelIntent: { groupCancelId: expect.any(String) } },
          });
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: parentId,
          commandId: CommandId.make("rg-late-lower"),
          runtimeMode: "auto",
        });
        const reconnected = yield* a.reconnect();
        yield* Context.get(reconnected.services, RemoteDelegation.RemoteDelegation).start();
        for (const task of members)
          yield* recorded(
            a,
            parentId,
            task.id,
            (current) =>
              current.remoteChild?.stop?.state === "refused" &&
              current.remoteChild.cancelResolution?.outcome === "refused",
          );
        for (const [index, task] of members.entries())
          yield* finishLifecycleRun(b, childOf(task), `rg-late-${index}`);
        for (const task of members)
          yield* recorded(a, parentId, task.id, (current) => current.result !== null);
        expect((yield* committedWakes(a, parentId, ids(members))).size).toBe(1);
        expect(yield* destinationStops(b)).toBe(0);
      }),
    ).pipe(Effect.provide(services)),
);

/** Threads B created for launches from elsewhere. */
const destinationThreads = (node: Node) =>
  Context.get(node.database, SqlClient.SqlClient)<{
    readonly count: number;
  }>`SELECT COUNT(*) AS count FROM orchestration_events WHERE event_type='thread.created' AND json_type(payload_json,'$.delegatedFrom')='object'`.pipe(
    Effect.map((rows) => rows[0]?.count ?? 0),
  );

it.live(
  "runGroups751: members launch with the kit read at selection, each on its own follower",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parentId } = yield* pair("rg-kit");
        yield* Ref.set(a.holdLaunchRequests, true);
        // The call answers while both launches still wait to be sent: nothing it awaited
        // needed the network after the group was recorded, and neither member waits on the other.
        const receipt = yield* nativeCall(a, parentId, "delegate_task", {
          task: "Review the plan.",
          prismRole: "reviewer",
          target: { environmentId: b.environmentId, projectId: b.project.id },
          clientRequestId: "rg-kit",
        });
        expect(receipt.isError, json(receipt.content)).not.toBe(true);
        const held = [
          yield* Queue.take(a.launchRequestsHeld),
          yield* Queue.take(a.launchRequestsHeld),
        ];
        expect(new Set(held).size).toBe(2);
        yield* b.fixtureSettings.updateSettings({
          prismRoles: { reviewer: { instructions: "B changed kit." } },
        } as never);
        yield* Deferred.succeed(a.releaseLaunchRequests, undefined);
        const group = yield* decodeGroup(receipt.structuredContent);
        for (const task of group.tasks) {
          const member = yield* recorded(
            a,
            parentId,
            task.taskId,
            (current) => current.remoteChild?.threadId !== undefined,
          );
          const child = yield* b.threads.getThreadRecords(childOf(member), ["messages"]);
          expect(child.messages[0]?.text).toBe("B review kit.\n\nReview the plan.");
        }
      }),
    ).pipe(Effect.provide(services)),
);

it.live("runGroups751: mode wait returns the finished group, or wakes once after a timeout", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { a, b, parentId } = yield* pair("rg-wait");
      const waiting = yield* nativeCall(a, parentId, "delegate_task", {
        task: "Review the plan.",
        prismRole: "reviewer",
        target: { environmentId: b.environmentId, projectId: b.project.id },
        clientRequestId: "rg-wait",
        mode: "wait",
        timeoutMs: 600_000,
      }).pipe(Effect.forkScoped);
      // The group is recorded before the wait starts; its members arrive as events.
      const seen = new Map<string, OrchestrationV2Subagent>();
      const members = yield* a.orchestrator
        .streamStoredEventsFrom({
          threadId: parentId,
          afterSequence: 0,
          eventType: "subagent.updated",
        })
        .pipe(
          Stream.filter((stored) => {
            if (stored.event.type !== "subagent.updated") return false;
            const task = stored.event.payload;
            if (task.runGroupId !== undefined) seen.set(task.id, task);
            return seen.size === 2;
          }),
          Stream.runHead,
          Effect.map(() => [...seen.values()]),
        );
      const attached = yield* Effect.forEach(members, (task) =>
        recorded(a, parentId, task.id, (current) => current.remoteChild?.threadId !== undefined),
      );
      for (const [index, task] of attached.entries())
        yield* finishLifecycleRun(b, childOf(task), `rg-wait-${index}`);
      const waited = yield* decodeGroup((yield* Fiber.join(waiting)).structuredContent);
      expect(waited.status).toBe("completed");
      expect(waited.waitTimedOut).toBe(false);
      // A wait that ran out delivered nothing; the group then wakes its parent once.
      expect((yield* committedWakes(a, parentId, ids(attached))).size).toBe(0);

      const timedOut = yield* decodeGroup(
        (yield* nativeCall(a, parentId, "delegate_task", {
          task: "Review again.",
          prismRole: "reviewer",
          target: { environmentId: b.environmentId, projectId: b.project.id },
          clientRequestId: "rg-wait-timeout",
          mode: "wait",
          timeoutMs: 1,
        })).structuredContent,
      );
      expect(timedOut.waitTimedOut).toBe(true);
      const later = yield* Effect.forEach(timedOut.tasks, (task) =>
        recorded(
          a,
          parentId,
          task.taskId,
          (current) => current.remoteChild?.threadId !== undefined,
        ),
      );
      for (const [index, task] of later.entries())
        yield* finishLifecycleRun(b, childOf(task), `rg-wait-later-${index}`);
      for (const task of later)
        yield* recorded(a, parentId, task.id, (current) => current.result !== null);
      expect((yield* committedWakes(a, parentId, ids(later))).size).toBe(1);
    }),
  ).pipe(Effect.provide(services)),
);

it.live("runGroups751: a group runs the Colors that can start there, and none refuses it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { a, b, parentId } = yield* pair("rg-short");
      yield* b.fixtureSettings.updateSettings({
        prismRoles: {
          reviewer: {
            models: [
              { instanceId: "codex", model: "gpt-5.4" },
              { instanceId: "codex", model: "not-advertised" },
            ],
          },
        },
      } as never);
      const short = yield* startGroup(a, b, parentId, "rg-short");
      expect(short.members.map((task) => task.remoteChild!.launch!.modelSelection!.model)).toEqual([
        "gpt-5.4",
      ]);
      yield* b.fixtureSettings.updateSettings({
        prismRoles: { reviewer: { models: [{ instanceId: "codex", model: "not-advertised" }] } },
      } as never);
      const threadsBefore = yield* destinationThreads(b);
      const none = yield* nativeCall(a, parentId, "delegate_task", {
        task: "Review the plan.",
        prismRole: "reviewer",
        target: { environmentId: b.environmentId, projectId: b.project.id },
        clientRequestId: "rg-none",
      });
      expect(none.isError).toBe(true);
      expect(json(none.content)).toContain("No eligible model with usage headroom");
      expect(yield* tasksOf(a, parentId)).toHaveLength(1);
      expect(yield* destinationThreads(b)).toBe(threadsBefore);
    }),
  ).pipe(Effect.provide(services)),
);

it.live(
  "runGroups751: a retry replays the group under its purpose; a changed purpose is refused",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parentId } = yield* pair("rg-purpose");
        const purposeIntent = (number: number) => ({
          version: 1,
          intent: {
            type: "reference",
            reference: {
              host: "github.com",
              repository: "toolboxmd/chromeria",
              kind: "issue",
              number,
            },
          },
        });
        const first = yield* startGroup(a, b, parentId, "rg-purpose", {
          purposeIntent: purposeIntent(751),
        });
        // Each member's launch carries its own purpose launch id, its seed in the group's command.
        const groupCommandId = first.groupId.slice("run-group:".length);
        for (const [index, task] of first.members.entries())
          expect(task.remoteChild!.launch!.delegatedFrom.purposeLaunch?.launchId).toBe(
            `${groupCommandId}:member:${index}`,
          );
        const threadsBefore = yield* destinationThreads(b);
        const retry = yield* nativeCall(a, parentId, "delegate_task", {
          task: "Review the plan.",
          prismRole: "reviewer",
          target: { environmentId: b.environmentId, projectId: b.project.id },
          clientRequestId: "rg-purpose",
          purposeIntent: purposeIntent(751),
        });
        expect((yield* decodeGroup(retry.structuredContent)).groupId).toBe(first.groupId);
        const changed = yield* nativeCall(a, parentId, "delegate_task", {
          task: "Review the plan.",
          prismRole: "reviewer",
          target: { environmentId: b.environmentId, projectId: b.project.id },
          clientRequestId: "rg-purpose",
          purposeIntent: purposeIntent(752),
        });
        expect(changed.isError).toBe(true);
        expect(json(changed.content)).toContain("intent");
        expect(yield* tasksOf(a, parentId)).toHaveLength(2);
        expect(yield* destinationThreads(b)).toBe(threadsBefore);
      }),
    ).pipe(Effect.provide(services)),
);

it.live("runGroups751: an older destination refuses the group before anything is recorded", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { a, b, parentId } = yield* pair("rg-older");
      yield* Ref.set(a.legacyRunGroupPeer, true);
      const refused = yield* nativeCall(a, parentId, "delegate_task", {
        task: "Review the plan.",
        prismRole: "reviewer",
        target: { environmentId: b.environmentId, projectId: b.project.id },
        clientRequestId: "rg-older",
      });
      expect(refused.isError).toBe(true);
      expect(json(refused.content)).toContain("cannot run a Run group");
      expect(yield* tasksOf(a, parentId)).toHaveLength(0);
      expect(yield* destinationThreads(b)).toBe(0);
    }),
  ).pipe(Effect.provide(services)),
);

it.live(
  "runGroups751: only a Run group launch from its delegation owner carries a member kit",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parentId } = yield* pair("rg-provenance");
        const launch = {
          projectId: b.project.id,
          title: "Forged member",
          message: "Review the plan.",
          prismRole: "reviewer",
          modelSelection: { instanceId: "codex", model: "gpt-5.4" },
          runGroupMember: { kitText: "Forged kit." },
          clientRequestId: "rg-forged",
        };
        // An agent here cannot send one: refused before anything is forwarded.
        const forwarded = yield* nativeCall(a, parentId, "t3_thread_launch", {
          ...launch,
          environmentId: b.environmentId,
        });
        expect(forwarded.isError).toBe(true);
        expect(json(forwarded.content)).toContain("runGroupMember");
        // An agent there cannot either.
        const bParent = ThreadId.make("rg-provenance-b-parent");
        yield* seedParent(b, bParent);
        const local = yield* nativeCall(b, bParent, "t3_thread_launch", launch);
        expect(local.isError).toBe(true);
        // A linked caller without the delegation's pair, or with another role or no Color, neither.
        const scope = caller(a, parentId);
        const delegatedFrom = { environmentId: a.environmentId, threadId: parentId, title: "A" };
        for (const args of [
          { ...launch, delegatedFrom },
          { ...launch, delegatedFrom, prismRole: "worker", clientRequestId: "rg-forged-worker" },
        ]) {
          const result = yield* a.forwarding
            .call(scope, ProjectToolkit.tools.t3_thread_launch, b.environmentId, args as never)
            .pipe(Effect.result);
          expect(result._tag).toBe("Failure");
        }
        const { modelSelection: _color, ...uncolored } = launch;
        const noColor = yield* a.forwarding
          .call(
            scope,
            ProjectToolkit.tools.t3_thread_launch,
            b.environmentId,
            { ...uncolored, delegatedFrom, clientRequestId: "rg-forged-color" } as never,
            {
              expectedLinkIdentity: Option.getOrThrow(yield* a.links.get(b.environmentId))
                .launchIdentity,
              pair: { parentThreadId: parentId, taskId: NodeId.make("rg-forged-task") },
            },
          )
          .pipe(Effect.result);
        expect(noColor._tag).toBe("Failure");
        expect(yield* destinationThreads(b)).toBe(0);
      }),
    ).pipe(Effect.provide(services)),
);

const namespaceOf = (a: Node, parentId: ThreadId) => `thread:${a.environmentId}:${parentId}`;

/** A group cancel committed with no member entry after it, as a crash right after it leaves. */
const admitGroupCancel = (
  a: Node,
  parentId: ThreadId,
  groupId: NodeId,
  members: ReadonlyArray<OrchestrationV2Subagent>,
  key: string,
  reason?: string,
) =>
  a.threads.dispatch({
    type: "delegated_task.group-cancel",
    commandId: CommandId.make(`command:rg-admit:${key}`),
    parentThreadId: parentId,
    groupId,
    remoteMembers: members.map((task, index) => ({
      taskId: task.id,
      cancelRequestId: RemoteDelegation.cancelRequestId(
        namespaceOf(a, parentId),
        `${key}:${index}`,
      ),
      ...(reason === undefined ? {} : { reason }),
    })),
  });

/** A member's own entry, as the group cancel makes it, or an individual cancel with its key. */
const memberEntry = (
  a: Node,
  parentId: ThreadId,
  task: OrchestrationV2Subagent,
  key: string,
  index: number,
  reason?: string,
) =>
  nativeCall(a, parentId, "task_cancel", {
    taskId: task.id,
    clientRequestId: `${key}:${index}`,
    ...(reason === undefined ? {} : { reason }),
  });

/** How many times a Stop with this identity was recorded here: each time it became current. */
const stopRecords = (a: Node, parentId: ThreadId, taskId: NodeId, requestId: string) =>
  Context.get(a.database, SqlClient.SqlClient)<{
    readonly count: number;
  }>`SELECT COUNT(*) AS count FROM (SELECT json_extract(payload_json,'$.remoteChild.stop.requestId') AS request_id, LAG(json_extract(payload_json,'$.remoteChild.stop.requestId')) OVER (ORDER BY sequence) AS prior_request FROM orchestration_events WHERE stream_id=${parentId} AND event_type='subagent.updated' AND json_extract(payload_json,'$.id')=${taskId}) WHERE request_id=${requestId} AND prior_request IS NOT ${requestId}`.pipe(
    Effect.map((rows) => rows[0]?.count ?? 0),
  );

const restart = Effect.fn("runGroups.restart")(function* (a: Node) {
  const reconnected = yield* a.reconnect();
  yield* Context.get(reconnected.services, RemoteDelegation.RemoteDelegation).start();
});

it.live("runGroups751: recovery racing a member's entry records and sends its Stop once", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { a, b, parentId } = yield* pair("rg-race");
      const { groupId, members } = yield* startGroup(a, b, parentId, "rg-race");
      yield* admitGroupCancel(a, parentId, groupId, members, "rg-race-stop", "Enough");
      // The followers wake on the intents and recover while each entry runs.
      const entries = yield* Effect.forEach(
        members,
        (task, index) => memberEntry(a, parentId, task, "rg-race-stop", index, "Enough"),
        { concurrency: "unbounded" },
      );
      for (const entry of entries) expect(entry.isError, json(entry.content)).not.toBe(true);
      for (const task of members)
        yield* recorded(a, parentId, task.id, (current) => current.groupCancel !== undefined);
      for (const [index, task] of members.entries())
        expect(
          yield* stopRecords(
            a,
            parentId,
            task.id,
            RemoteDelegation.cancelRequestId(namespaceOf(a, parentId), `rg-race-stop:${index}`),
          ),
        ).toBe(1);
      expect(yield* destinationStops(b)).toBe(2);
      expect((yield* committedWakes(a, parentId, ids(members))).size).toBe(0);
    }),
  ).pipe(Effect.provide(services)),
);

it.live(
  "runGroups751: a group cancel superseded before its entries replays as a no-op after a restart",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parentId } = yield* pair("rg-supersede");
        const { groupId, members } = yield* startGroup(a, b, parentId, "rg-supersede");
        yield* a.stopConnections;
        yield* admitGroupCancel(a, parentId, groupId, members, "g1", "Enough");
        yield* admitGroupCancel(a, parentId, groupId, members, "g2", "Enough");
        // G2 replaced G1's intents in the same events that recorded G1 as superseded.
        for (const [index, task] of members.entries())
          yield* recorded(
            a,
            parentId,
            task.id,
            (current) =>
              current.remoteChild?.cancelIntent?.requestId ===
                RemoteDelegation.cancelRequestId(namespaceOf(a, parentId), `g2:${index}`) &&
              current.remoteChild.cancelResolution?.requestId ===
                RemoteDelegation.cancelRequestId(namespaceOf(a, parentId), `g1:${index}`) &&
              current.remoteChild.cancelResolution.outcome === "superseded",
          );
        yield* restart(a);
        for (const task of members)
          yield* recorded(a, parentId, task.id, (current) => current.groupCancel !== undefined);
        const stops = yield* destinationStops(b);
        const g1 = (index: number) =>
          RemoteDelegation.cancelRequestId(namespaceOf(a, parentId), `g1:${index}`);
        const recordedG1 = Effect.forEach(members, (task, index) =>
          stopRecords(a, parentId, task.id, g1(index)),
        );
        const recordedBefore = yield* recordedG1;
        // G1's delayed entries find their admission: nothing is recorded or sent again.
        for (const [index, task] of members.entries()) {
          const replay = yield* memberEntry(a, parentId, task, "g1", index, "Enough");
          expect(replay.isError, json(replay.content)).not.toBe(true);
        }
        const changed = yield* memberEntry(a, parentId, members[0]!, "g1", 0, "Other reason");
        expect(changed.isError).toBe(true);
        expect(yield* recordedG1).toEqual(recordedBefore);
        expect(yield* destinationStops(b)).toBe(stops);
        expect((yield* committedWakes(a, parentId, ids(members))).size).toBe(0);
      }),
    ).pipe(Effect.provide(services)),
);

it.live.each([
  { outcome: "accepted", restart: true },
  { outcome: "refused", restart: true },
  { outcome: "accepted", restart: false },
  { outcome: "refused", restart: false },
] as const)(
  "runGroups751: an owed Stop $outcome before a member's entry resolves its group cancel (restart: $restart)",
  ({ outcome, restart: restarted }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = `rg-owed-${outcome}-${restarted}`;
        const { a, b, parentId } = yield* pair(name);
        const { groupId, members } = yield* startGroup(a, b, parentId, name);
        for (const [index, task] of members.entries())
          yield* finishLifecycleRun(b, childOf(task), `${name}-${index}`);
        for (const task of members)
          yield* recorded(a, parentId, task.id, (current) => current.result !== null);
        const wakes = (yield* committedWakes(a, parentId, ids(members))).size;
        expect(wakes).toBe(1);
        if (outcome === "refused")
          yield* a.threads.dispatch({
            type: "thread.runtime-mode.set",
            threadId: parentId,
            commandId: CommandId.make(`${name}-lower`),
            runtimeMode: "auto",
          });
        // A Stop owed for later work there is current and unanswered when the group is cancelled.
        yield* a.stopConnections;
        yield* a.threads.dispatch({
          type: "delegated_task.remote.stop",
          commandId: CommandId.make(`command:${name}`),
          parentThreadId: parentId,
          taskId: members[0]!.id,
        });
        const owed = (yield* recorded(
          a,
          parentId,
          members[0]!.id,
          (task) => task.remoteChild?.stop?.state === "pending",
        )).remoteChild!.stop!;
        yield* admitGroupCancel(a, parentId, groupId, members, "rg-owed-stop");
        // Its answer arrives through a restarted follower there, or straight from the
        // destination's reply to the attempt already running.
        if (restarted) yield* restart(a);
        else
          yield* a.orchestrator.dispatch({
            type: "delegated_task.remote.observe",
            commandId: CommandId.make(`command:${name}:answer`),
            parentThreadId: parentId,
            taskId: members[0]!.id,
            stop:
              outcome === "accepted"
                ? {
                    ...owed,
                    state: "acknowledged",
                    destinationCommandId: CommandId.make(`${name}-d`),
                  }
                : { ...owed, state: "refused", refusal: "runtime_mode" },
          });
        const owedIntent = RemoteDelegation.cancelRequestId(
          namespaceOf(a, parentId),
          "rg-owed-stop:0",
        );
        yield* recorded(
          a,
          parentId,
          members[0]!.id,
          (current) =>
            current.remoteChild?.cancelResolution?.requestId === owedIntent &&
            current.remoteChild.cancelResolution.outcome ===
              (outcome === "accepted" ? "applied" : "refused"),
        );
        const stops = yield* destinationStops(b);
        const entry = yield* memberEntry(a, parentId, members[0]!, "rg-owed-stop", 0);
        expect(entry.isError).toBe(outcome === "refused");
        // A changed reason for the same identity is refused, with no effect there.
        const changed = yield* memberEntry(a, parentId, members[0]!, "rg-owed-stop", 0, "Other");
        expect(changed.isError).toBe(true);
        // The owed Stop resolved the cancel; the cancel's own Stop is never recorded or sent.
        expect(yield* stopRecords(a, parentId, members[0]!.id, owedIntent)).toBe(0);
        expect(yield* destinationStops(b)).toBe(stops);
        // Accepted for both members (the other's own Stop is answered after the restart), the
        // cancel gives up the wake they had not yet delivered, as a local group cancel of finished
        // members does; while a member's Stop is unanswered, or refused, the wake stands.
        expect((yield* committedWakes(a, parentId, ids(members))).size).toBe(
          outcome === "accepted" && restarted ? 0 : wakes,
        );
      }),
    ).pipe(Effect.provide(services)),
);

/**
 * Records a two-member group whose members have no follower yet, as one recorded just before a
 * crash: neither launch has been sent.
 */
const recordUnsent = Effect.fn("runGroups.recordUnsent")(function* (
  a: Node,
  b: Node,
  parentId: ThreadId,
  key: string,
) {
  const link = Option.getOrThrow(yield* a.links.get(b.environmentId));
  const parentRun = (yield* a.threads.getThreadRecords(parentId, ["runs"])).runs.find(
    (run) => run.rootNodeId !== null && run.completedAt === null,
  )!;
  const commandId = CommandId.make(`command:rg-unsent:${key}`);
  const groupId = NodeId.make(`run-group:${commandId}`);
  const members = (["gpt-5.4", "gpt-5"] as const).map((model, index) => {
    const modelSelection = { instanceId: instanceId, model };
    return {
      modelSelection,
      remoteChild: {
        environmentId: b.environmentId,
        label: link.label,
        title: key,
        launch: {
          lifecycleVersion: 1 as const,
          requestNamespace: namespaceOf(a, parentId),
          clientRequestId: `delegate-group:${parentId}:${key}:${index}`,
          linkIdentity: link.launchIdentity,
          capabilities: ["orchestration" as const],
          projectId: b.project.id,
          title: key,
          modelSelection,
          prismRole: "reviewer" as const,
          runtimeMode: "full-access" as const,
          interactionMode: "default" as const,
          delegatedFrom: { environmentId: a.environmentId, threadId: parentId, title: parentId },
          runGroupMember: { kitText: "B review kit." },
        },
      },
    };
  });
  yield* a.threads.dispatch({
    type: "delegated_task.remote.group.request",
    commandId,
    parentThreadId: parentId,
    parentRunId: parentRun.id,
    parentNodeId: parentRun.rootNodeId!,
    groupId,
    task: "Review the plan.",
    driver: ProviderDriverKind.make("remote"),
    members: [members[0]!, members[1]!],
    completionWake: "always",
  });
  return {
    groupId,
    members: yield* tasksOf(a, parentId).pipe(
      Effect.map((tasks) => tasks.filter((task) => task.runGroupId === groupId)),
    ),
  };
});

it.live(
  "runGroups751: the last member settling as never started releases a held sibling's wake once",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parentId } = yield* pair("rg-unsent");
        const { members } = yield* recordUnsent(a, b, parentId, "rg-unsent");
        const [first, second] = members;
        yield* a.remote.ensureFollowing(parentId, [second!.id]);
        const ran = yield* recorded(
          a,
          parentId,
          second!.id,
          (task) => task.remoteChild?.threadId !== undefined,
        );
        yield* finishLifecycleRun(b, childOf(ran), "rg-unsent-result");
        // Held for the group while its sibling has not ended.
        yield* recorded(a, parentId, second!.id, (task) => task.result === "rg-unsent-result");
        expect((yield* committedWakes(a, parentId, ids(members))).size).toBe(0);
        const cancel = yield* nativeCall(a, parentId, "task_cancel", {
          taskId: first!.id,
          reason: "Not needed",
        });
        expect(cancel.structuredContent).toMatchObject({ stopState: "not_started" });
        yield* recorded(a, parentId, first!.id, (task) => task.status === "cancelled");
        const parent = yield* a.orchestrator.getThreadProjection(parentId);
        const delivery = parent.runs.flatMap((run) => run.delegatedCompletion?.delivery ?? [])[0];
        expect([...(delivery?.taskIds ?? [])].toSorted()).toEqual(ids(members).toSorted());
        expect((yield* committedWakes(a, parentId, ids(members))).size).toBe(1);
        expect(yield* destinationThreads(b)).toBe(1);
      }),
    ).pipe(Effect.provide(services)),
);

it.live.each(["before", "between"] as const)(
  "runGroups751: a restart %s member entries settles never-started members once, quietly",
  (when) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parentId } = yield* pair(`rg-restart-${when}`);
        const { groupId, members } = yield* recordUnsent(a, b, parentId, `rg-restart-${when}`);
        yield* admitGroupCancel(a, parentId, groupId, members, "rg-restart-stop", "Enough");
        if (when === "between") {
          const entry = yield* memberEntry(
            a,
            parentId,
            members[0]!,
            "rg-restart-stop",
            0,
            "Enough",
          );
          expect(entry.isError, json(entry.content)).not.toBe(true);
        }
        yield* a.stopConnections;
        yield* restart(a);
        for (const task of members)
          yield* recorded(
            a,
            parentId,
            task.id,
            (current) =>
              current.status === "cancelled" &&
              current.groupCancel !== undefined &&
              current.remoteChild?.stop?.state === "not_started",
          );
        // Delayed entries find their admission; nothing is recorded, launched or sent.
        for (const [index, task] of members.entries()) {
          const entry = yield* memberEntry(a, parentId, task, "rg-restart-stop", index, "Enough");
          expect(entry.isError, json(entry.content)).not.toBe(true);
          expect(
            yield* stopRecords(
              a,
              parentId,
              task.id,
              RemoteDelegation.cancelRequestId(
                namespaceOf(a, parentId),
                `rg-restart-stop:${index}`,
              ),
            ),
          ).toBe(1);
        }
        expect((yield* committedWakes(a, parentId, ids(members))).size).toBe(0);
        expect(yield* destinationThreads(b)).toBe(0);
        expect(yield* destinationStops(b)).toBe(0);
      }),
    ).pipe(Effect.provide(services)),
);

it.live("runGroups751: a member's nested work there keeps the group open until it ends", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { a, b, parentId } = yield* pair("rg-nested");
      const { members } = yield* startGroup(a, b, parentId, "rg-nested");
      const [first, second] = members;
      const run = yield* activate(b, childOf(first!));
      const nestedReceipt = yield* nativeCall(b, childOf(first!), "delegate_task", {
        task: "Nested work",
        target: { providerInstanceId: instanceId, model: "gpt-5.4" },
        clientRequestId: "rg-nested-inner",
      });
      expect(nestedReceipt.isError, json(nestedReceipt.content)).not.toBe(true);
      const { taskId: nestedTask } = yield* decodeTaskReceipt(nestedReceipt.structuredContent);
      const nested = (yield* b.threads.getThreadRecords(childOf(first!), [
        "subagents",
      ])).subagents.find((task) => task.id === nestedTask)!;
      yield* activate(b, nested.childThreadId!);
      yield* finishLifecycleRun(b, childOf(first!), "rg-nested-outer", "completed", "Outer", run);
      yield* finishLifecycleRun(b, childOf(second!), "rg-nested-sibling");
      yield* recorded(a, parentId, second!.id, (task) => task.result !== null);
      // The member's run there ended, but its nested work did not: it is still running here.
      yield* recorded(
        a,
        parentId,
        first!.id,
        (task) => (task.remoteChild?.snapshot?.pendingChildren ?? 0) > 0,
      );
      expect(
        (yield* tasksOf(a, parentId)).find((task) => task.id === first!.id)?.result,
      ).toBeNull();
      expect((yield* committedWakes(a, parentId, ids(members))).size).toBe(0);
      // Its nested work ends there, through the thread's own cancel.
      const ended = yield* nativeCall(b, childOf(first!), "task_cancel", { taskId: nestedTask });
      expect(ended.isError, json(ended.content)).not.toBe(true);
      yield* recorded(a, parentId, first!.id, (task) => task.result !== null);
      expect((yield* committedWakes(a, parentId, ids(members))).size).toBe(1);
    }),
  ).pipe(Effect.provide(services)),
);

it.live(
  "runGroups751: a plain capabilities read is unchanged and says Run groups are supported",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parentId } = yield* pair("rg-read");
        const plain = yield* a.forwarding.call(
          caller(a, parentId),
          OrchestratorToolkit.tools.orchestrator_capabilities,
          b.environmentId,
          {},
        );
        expect(plain.runGroupColors).toBe(1);
        expect(plain.prismColors).toBeUndefined();
        expect(plain.providers.length).toBeGreaterThan(0);
      }),
    ).pipe(Effect.provide(services)),
);

it.live.each(["accepted", "refused", "changed-reason"] as const)(
  "runGroups751: a member's own cancel before the group cancel keeps its outcome (%s)",
  (variant) =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = `rg-first-${variant}`;
        const { a, b, parentId } = yield* pair(name);
        const { groupId, members } = yield* startGroup(a, b, parentId, name);
        if (variant === "refused")
          yield* a.threads.dispatch({
            type: "thread.runtime-mode.set",
            threadId: parentId,
            commandId: CommandId.make(`${name}-lower`),
            runtimeMode: "auto",
          });
        // The member's own cancel uses the identity the group cancel derives for it.
        const own = yield* memberEntry(a, parentId, members[0]!, "G", 0, "Enough");
        expect(own.isError).toBe(variant === "refused");
        const identity = RemoteDelegation.cancelRequestId(namespaceOf(a, parentId), "G:0");
        yield* recorded(
          a,
          parentId,
          members[0]!.id,
          (task) =>
            task.remoteChild?.stop?.requestId === identity &&
            task.remoteChild.stop.state !== "pending" &&
            task.remoteChild.cancelIntent === undefined,
        );
        const stops = yield* destinationStops(b);
        const group = yield* nativeCall(a, parentId, "task_cancel", {
          taskId: groupId,
          clientRequestId: "G",
          reason: variant === "changed-reason" ? "Other" : "Enough",
        });
        expect(group.isError).toBe(variant !== "accepted");
        const first = (yield* tasksOf(a, parentId)).find((task) => task.id === members[0]!.id)!;
        // The recorded identity never reopens: no intent, and no second Stop for it.
        expect(first.remoteChild?.cancelIntent).toBeUndefined();
        expect(yield* stopRecords(a, parentId, members[0]!.id, identity)).toBe(1);
        expect(first.groupCancel !== undefined).toBe(variant === "accepted");
        if (variant === "accepted") expect(first.completionDelivery?.state).toBe("disposed");
        // The other member gets the group's intent and its own Stop (refused while lowered).
        yield* recorded(
          a,
          parentId,
          members[1]!.id,
          (task) => task.remoteChild?.cancelResolution !== undefined,
        );
        expect(yield* destinationStops(b)).toBe(stops + (variant === "refused" ? 0 : 1));
        // A restart finds nothing outstanding for the first member.
        yield* a.stopConnections;
        yield* restart(a);
        const after = (yield* tasksOf(a, parentId)).find((task) => task.id === members[0]!.id)!;
        expect(after.remoteChild?.cancelIntent).toBeUndefined();
        expect(yield* a.projections.getOpenRemoteDelegatedTasks).toContainEqual({
          parentThreadId: parentId,
          taskId: members[0]!.id,
        });
        expect(yield* destinationStops(b)).toBe(stops + (variant === "refused" ? 0 : 1));
      }),
    ).pipe(Effect.provide(services)),
);

it.live("runGroups751: competing recoveries and entries record an admitted Stop once", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { a, b, parentId } = yield* pair("rg-contend");
      const { groupId, members } = yield* recordUnsent(a, b, parentId, "rg-contend");
      yield* admitGroupCancel(a, parentId, groupId, members, "rg-contend-stop", "Enough");
      const identity = RemoteDelegation.cancelRequestId(
        namespaceOf(a, parentId),
        "rg-contend-stop:0",
      );
      // Every contender passed its own check before any committed; the parent lock orders them.
      const observe = (label: string, recovery: boolean) =>
        a.orchestrator.dispatch({
          type: "delegated_task.remote.observe",
          commandId: CommandId.make(`command:rg-contend:${label}`),
          parentThreadId: parentId,
          taskId: members[0]!.id,
          stop: { requestId: identity, state: "pending", reason: "Enough" },
          cancel: true,
          ...(recovery ? { recovery: true as const } : {}),
        });
      yield* Effect.all(
        [
          observe("recover-1", true),
          observe("entry-1", false),
          observe("recover-2", true),
          observe("entry-2", false),
          observe("recover-3", true),
        ],
        { concurrency: "unbounded" },
      );
      const sql = Context.get(a.database, SqlClient.SqlClient);
      const records = yield* sql<{
        readonly count: number;
      }>`SELECT COUNT(*) AS count FROM orchestration_events WHERE stream_id=${parentId} AND event_type='subagent.updated' AND json_extract(payload_json,'$.id')=${members[0]!.id} AND json_extract(payload_json,'$.remoteChild.stop.requestId')=${identity}`;
      expect(records[0]?.count).toBe(1);
      const settled = (yield* tasksOf(a, parentId)).find((task) => task.id === members[0]!.id)!;
      expect(settled).toMatchObject({
        status: "cancelled",
        remoteChild: { stop: { requestId: identity, state: "not_started" } },
      });
      expect(yield* destinationThreads(b)).toBe(0);
      expect(yield* destinationStops(b)).toBe(0);
    }),
  ).pipe(Effect.provide(services)),
);

it.live(
  "runGroups751: a quiet member whose wake was read recovers the group cancel after a restart",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parentId } = yield* pair("rg-quiet");
        const { groupId, members } = yield* startGroup(a, b, parentId, "rg-quiet");
        for (const [index, task] of members.entries())
          yield* finishLifecycleRun(b, childOf(task), `rg-quiet-${index}`);
        for (const task of members)
          yield* recorded(a, parentId, task.id, (current) => current.result !== null);
        // The parent consumed the group's wake: nothing is left to deliver, so they wait quietly.
        for (const task of members)
          yield* a.threads.dispatch({
            type: "delegated_task.completion-delivery.dispose",
            commandId: CommandId.make(`rg-quiet-read-${task.id}`),
            parentThreadId: parentId,
            taskId: task.id,
          });
        const wakes = (yield* committedWakes(a, parentId, ids(members))).size;
        yield* a.stopConnections;
        yield* admitGroupCancel(a, parentId, groupId, members, "rg-quiet-stop");
        yield* restart(a);
        for (const task of members)
          yield* recorded(
            a,
            parentId,
            task.id,
            (current) =>
              current.groupCancel !== undefined &&
              current.remoteChild?.cancelResolution?.outcome === "applied",
          );
        // One Stop each there, and no further wake.
        expect(yield* destinationStops(b)).toBe(2);
        expect((yield* committedWakes(a, parentId, ids(members))).size).toBe(wakes);
      }),
    ).pipe(Effect.provide(services)),
);

it.live("runGroups751: a destination rejects an argument it does not declare", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { a, b } = yield* pair("rg-undeclared");
      // Sent as is, past this side's own encoding: a destination from before Run groups
      // declares no runGroupMember, and refuses it as it refuses this.
      const connected = yield* a.reconnect();
      const refused = yield* connected.peers
        .call({
          environmentId: b.environmentId,
          tool: "t3_thread_launch",
          arguments: {
            projectId: b.project.id,
            title: "Undeclared",
            message: "Review the plan.",
            notDeclaredHere: { kitText: "Kit." },
          },
          limits: { runtimeMode: "full-access", interactionMode: "default" },
          success: Schema.Struct({ threadId: ThreadId }),
        })
        .pipe(Effect.flip);
      expect(refused.message).toContain('Unknown argument "notDeclaredHere"');
      expect(yield* destinationThreads(b)).toBe(0);
    }),
  ).pipe(Effect.provide(services)),
);
