import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  EnvironmentId,
  ThreadId,
  NodeId,
  ProjectId,
  OrchestrationV2RemoteTaskLaunch,
  WS_METHODS,
  AuthOrchestrationOperateScope,
  type AuthSessionState,
  type PeerTaskView,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { subagentChildTarget } from "./threadRelationships.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { EnvironmentRegistry } from "../connection/registry.ts";
import { EnvironmentSupervisor } from "../connection/supervisor.ts";
import { AVAILABLE_CONNECTION_STATE } from "../connection/model.ts";
import type { RpcSession } from "../rpc/session.ts";
import { createRemoteTaskState, hasSourceRemoteTaskView } from "./remoteTasks.ts";

const decodeRemoteTaskLaunch = Schema.decodeUnknownEffect(OrchestrationV2RemoteTaskLaunch);

vi.mock("./session.ts", () => ({
  createEnvironmentSessionAtoms: () => ({ sessionStateAtom: sessions }),
}));
const sessions = Atom.family((_id: EnvironmentId) =>
  Atom.make<AsyncResult.AsyncResult<AuthSessionState, string>>(AsyncResult.initial()),
);
it.effect(
  "reads a qualified destination through only the source connection and guards Stop before transport",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const source = EnvironmentId.make("A");
        const destination = EnvironmentId.make("B-with-no-saved-client-profile");
        const parentThreadId = ThreadId.make("A-parent");
        const taskId = NodeId.make("A-task");
        const legacyRemote = {
          environmentId: destination,
          label: "B",
          threadId: ThreadId.make("B-child"),
          launch: {
            requestNamespace: "original-380",
            clientRequestId: "original-key",
            linkIdentity: "original-generation",
            projectId: ProjectId.make("B-project"),
            title: "Remote job",
            delegatedFrom: { environmentId: source, threadId: parentThreadId, title: "Parent" },
          },
        };
        const legacyTask = { childThreadId: null, remoteChild: legacyRemote };
        expect(hasSourceRemoteTaskView(legacyTask)).toBe(false);
        expect(subagentChildTarget(legacyTask)).toEqual({
          environmentId: destination,
          threadId: legacyRemote.threadId,
        });
        const negotiatedTask = {
          ...legacyTask,
          remoteChild: {
            ...legacyRemote,
            launch: { ...legacyRemote.launch, lifecycleVersion: 1 as const },
          },
        };
        expect(hasSourceRemoteTaskView(negotiatedTask)).toBe(true);
        const malformed = yield* decodeRemoteTaskLaunch({
          ...legacyRemote.launch,
          lifecycleVersion: 2,
        }).pipe(Effect.result);
        expect(malformed._tag).toBe("Failure");
        const accessed: EnvironmentId[] = [];
        const requests: Array<{ method: string; input: unknown }> = [];
        const view: PeerTaskView = {
          taskId,
          environmentId: destination,
          threadId: ThreadId.make("B-child"),
          snapshot: {
            sequence: 9,
            observedAt: "2026-10-17T00:00:00Z",
            title: "Work on B",
            workState: "waiting_for_children",
            pendingChildren: 1,
            queuedRuns: 2,
            recoveryWaiting: true,
            incomplete: false,
            executionSettled: false,
            pendingQuestions: 0,
            requestCount: 0,
            requests: [],
          },
          childrenTruncated: false,
          children: [],
          messages: [{ id: "B-message", runId: null, role: "assistant", text: "Original work" }],
          nextCursor: 1,
          nextTextOffset: 50000,
        };
        const rpc = {
          client: {
            [WS_METHODS.remoteTaskView]: (input: unknown) =>
              Effect.sync(() => {
                requests.push({ method: WS_METHODS.remoteTaskView, input });
                return view;
              }),
            [WS_METHODS.remoteTaskStop]: (input: unknown) =>
              Effect.sync(() => {
                requests.push({ method: WS_METHODS.remoteTaskStop, input });
                return { taskId, status: "cancel_requested", stopState: "pending" };
              }),
          },
        } as unknown as RpcSession;
        const supervisor = {
          target: { environmentId: source, label: "A" },
          state: yield* SubscriptionRef.make({
            ...AVAILABLE_CONNECTION_STATE,
            desired: true,
            network: "online" as const,
            phase: "connected" as const,
            attempt: 1,
            generation: 1,
          }),
          session: yield* SubscriptionRef.make(Option.some(rpc)),
        } as EnvironmentSupervisor["Service"];
        const service = {
          run: (
            id: EnvironmentId,
            effect: Effect.Effect<unknown, Error, EnvironmentSupervisor>,
          ) => {
            accessed.push(id);
            if (id !== source) return Effect.die("No destination credential is registered");
            return Effect.provideService(effect, EnvironmentSupervisor, supervisor);
          },
          followStream: (
            id: EnvironmentId,
            stream: Stream.Stream<unknown, Error, EnvironmentSupervisor>,
          ) => {
            accessed.push(id);
            if (id !== source) return Stream.die("No destination credential is registered");
            return Stream.provideService(stream, EnvironmentSupervisor, supervisor);
          },
        } as unknown as EnvironmentRegistry["Service"];
        const state = createRemoteTaskState(
          Atom.runtime(Layer.succeed(EnvironmentRegistry, service)),
        );
        const registry = AtomRegistry.make();
        yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()));
        const query = state.view({
          environmentId: source,
          input: { parentThreadId, taskId, after: 0 },
        });
        yield* AtomRegistry.mount(registry, query);
        const result = yield* AtomRegistry.getResult(registry, query, { suspendOnWaiting: true });
        expect(result).toEqual(view);
        expect(accessed.length).toBeGreaterThan(0);
        expect(accessed.every((id) => id === source)).toBe(true);
        const session: AuthSessionState = {
          authenticated: true,
          auth: {
            policy: "remote-reachable",
            bootstrapMethods: [],
            sessionMethods: [],
            sessionCookieName: "test",
          },
          scopes: [],
          permissions: [],
        };
        yield* AtomRegistry.mount(registry, sessions(source));
        registry.set(sessions(source), AsyncResult.success(session));
        const input = {
          environmentId: source,
          input: { parentThreadId, taskId, clientRequestId: "stop381" },
        };
        const denied = yield* Effect.promise(() => state.stop.run(registry, input));
        expect(denied._tag).toBe("Failure");
        expect(
          requests.filter((request) => request.method === WS_METHODS.remoteTaskStop),
        ).toHaveLength(0);
        registry.set(
          sessions(source),
          AsyncResult.success({
            ...session,
            scopes: [AuthOrchestrationOperateScope],
            permissions: [AuthOrchestrationOperateScope],
          }),
        );
        const accepted = yield* Effect.promise(() => state.stop.run(registry, input));
        expect(accepted._tag).toBe("Success");
        expect(requests).toEqual([
          { method: WS_METHODS.remoteTaskView, input: { parentThreadId, taskId, after: 0 } },
          { method: WS_METHODS.remoteTaskStop, input: input.input },
        ]);
        expect(accessed.every((id) => id === source)).toBe(true);
      }),
    ),
  10000,
);
