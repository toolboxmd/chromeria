import { assert, it } from "@effect/vitest";
import {
  AuthSessionId,
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2Command,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import { makeSubagentChildThread } from "./SubagentProjection.ts";
import { stampSessionPerson } from "./ThreadPeople.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for thread people"),
} as ProviderAdapterV2Shape;
const layerDatabase = SqlitePersistence.layerMemory;
const layerTest = Layer.mergeAll(
  layerDatabase,
  ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "thread-people" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: layerDatabase, runEffectWorker: false },
  ),
);

let commandCount = 0;
const nextCommandId = () => CommandId.make(`people-${++commandCount}`);

const createThread = (threadId: ThreadId, owner?: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: nextCommandId(),
      threadId,
      projectId: ProjectId.make("project:people"),
      title: "People",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
      ...(owner === undefined ? {} : { owner }),
    });
  });

type SharingInput =
  | { readonly type: "thread.share"; readonly coOwner: string; readonly actor: string }
  | { readonly type: "thread.unshare"; readonly actor: string }
  | { readonly type: "thread.leave"; readonly actor: string };

const sharing = (threadId: ThreadId, input: SharingInput) =>
  Orchestrator.OrchestratorV2.pipe(
    Effect.flatMap((orchestrator) =>
      orchestrator.dispatch({ ...input, commandId: nextCommandId(), threadId }),
    ),
    Effect.exit,
  );

const people = (threadId: ThreadId) =>
  ProjectionStore.ProjectionStoreV2.pipe(
    Effect.flatMap((projections) => projections.getThreadShell(threadId)),
    Effect.map((shell) => ({ owner: shell?.owner, coOwners: shell?.coOwners })),
  );

it.effect("an owner shares and unshares, and only a co-owner leaves", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:pauli");
    yield* createThread(threadId, "Pauli");
    assert.deepEqual(yield* people(threadId), { owner: "Pauli", coOwners: undefined });

    assert.isTrue(
      Exit.isSuccess(
        yield* sharing(threadId, { type: "thread.share", coOwner: "Luke", actor: "Pauli" }),
      ),
    );
    yield* sharing(threadId, { type: "thread.share", coOwner: "Luke", actor: "Pauli" });
    assert.deepEqual(yield* people(threadId), { owner: "Pauli", coOwners: ["Luke"] });

    const refusals: ReadonlyArray<SharingInput> = [
      { type: "thread.share", coOwner: "Pauli", actor: "Pauli" },
      { type: "thread.unshare", actor: "Luke" },
      { type: "thread.leave", actor: "Pauli" },
    ];
    for (const refused of refusals) {
      assert.isTrue(Exit.isFailure(yield* sharing(threadId, refused)), refused.type);
    }
    assert.deepEqual(yield* people(threadId), { owner: "Pauli", coOwners: ["Luke"] });

    assert.isTrue(
      Exit.isSuccess(yield* sharing(threadId, { type: "thread.leave", actor: "Luke" })),
    );
    assert.deepEqual(yield* people(threadId), { owner: "Pauli", coOwners: [] });
    assert.isTrue(
      Exit.isFailure(yield* sharing(threadId, { type: "thread.leave", actor: "Luke" })),
    );

    yield* sharing(threadId, { type: "thread.share", coOwner: "Luke", actor: "Pauli" });
    assert.isTrue(
      Exit.isSuccess(yield* sharing(threadId, { type: "thread.unshare", actor: "Pauli" })),
    );
    assert.deepEqual(yield* people(threadId), { owner: "Pauli", coOwners: [] });
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a thread without an owner belongs to the default person", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:unowned");
    yield* createThread(threadId);
    assert.isTrue(
      Exit.isFailure(
        yield* sharing(threadId, { type: "thread.share", coOwner: "Luke", actor: "Luke" }),
      ),
    );
    yield* sharing(threadId, { type: "thread.share", coOwner: "Pauli", actor: "Luke" });
    assert.isTrue(
      Exit.isFailure(yield* sharing(threadId, { type: "thread.unshare", actor: "Pauli" })),
    );
    assert.isTrue(
      Exit.isSuccess(yield* sharing(threadId, { type: "thread.unshare", actor: "Luke" })),
    );
    assert.deepEqual(yield* people(threadId), { owner: undefined, coOwners: [] });
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a fork belongs to the forking person and starts unshared", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const eventSink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const sourceThreadId = ThreadId.make("thread:fork-source");
    const providerThreadId = ProviderThreadId.make("provider-thread:fork-source");
    const runId = RunId.make("run:fork-source");
    yield* createThread(sourceThreadId, "Luke");
    yield* sharing(sourceThreadId, { type: "thread.share", coOwner: "Pauli", actor: "Luke" });
    yield* eventSink.write({
      events: [
        {
          id: EventId.make("fork-source-provider-thread"),
          type: "provider-thread.updated",
          threadId: sourceThreadId,
          occurredAt: now,
          payload: {
            id: providerThreadId,
            driver,
            providerInstanceId: instanceId,
            providerSessionId: null,
            appThreadId: sourceThreadId,
            ownerNodeId: null,
            nativeThreadRef: { driver, nativeId: "native-fork-source", strength: "strong" },
            nativeConversationHeadRef: null,
            status: "idle",
            firstRunOrdinal: 1,
            lastRunOrdinal: 1,
            handoffIds: [],
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
          },
        },
        {
          id: EventId.make("fork-source-run"),
          type: "run.created",
          threadId: sourceThreadId,
          runId,
          occurredAt: now,
          payload: {
            id: runId,
            threadId: sourceThreadId,
            ordinal: 1,
            providerInstanceId: instanceId,
            modelSelection,
            providerThreadId,
            userMessageId: MessageId.make("fork-source-message"),
            rootNodeId: null,
            activeAttemptId: null,
            status: "completed",
            queuePosition: null,
            requestedAt: now,
            startedAt: now,
            completedAt: now,
            checkpointId: null,
            contextHandoffId: null,
          },
        },
      ],
    });
    const forkAs = (targetThreadId: ThreadId, owner?: string) =>
      orchestrator.dispatch({
        type: "thread.fork",
        commandId: nextCommandId(),
        sourceThreadId,
        targetThreadId,
        sourcePoint: { type: "run", runId },
        createdBy: "user",
        creationSource: "web",
        ...(owner === undefined ? {} : { owner }),
      });

    yield* forkAs(ThreadId.make("thread:fork-pauli"), "Pauli");
    assert.deepEqual(yield* people(ThreadId.make("thread:fork-pauli")), {
      owner: "Pauli",
      coOwners: [],
    });
    yield* forkAs(ThreadId.make("thread:fork-unstamped"));
    assert.deepEqual(yield* people(ThreadId.make("thread:fork-unstamped")), {
      owner: null,
      coOwners: [],
    });
    assert.deepEqual(yield* people(sourceThreadId), { owner: "Luke", coOwners: ["Pauli"] });
  }).pipe(Effect.provide(layerTest)),
);

it.effect("subagent children belong to the parent's owner and start unshared", () =>
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const parentId = ThreadId.make("thread:parent");
    yield* createThread(parentId, "Pauli");
    yield* sharing(parentId, { type: "thread.share", coOwner: "Luke", actor: "Pauli" });
    const childOf = (parentThread: OrchestrationV2AppThread) =>
      makeSubagentChildThread({
        parentThread,
        childThreadId: ThreadId.make("thread:child"),
        parentNodeId: NodeId.make("node:child"),
        activeProviderThreadId: null,
        providerInstanceId: instanceId,
        modelSelection,
        title: "Child",
        now: DateTime.makeUnsafe("2026-01-01T00:00:00.000Z"),
        createdBy: "agent",
        creationSource: "mcp",
      });
    const child = childOf(yield* projections.getThread(parentId));
    assert.equal(child.owner, "Pauli");
    assert.deepEqual(child.coOwners, []);

    // A parent from before people has no owner field; its children name the default person.
    const {
      owner: _owner,
      coOwners: _coOwners,
      ...legacyParent
    } = yield* projections.getThread(parentId);
    const legacyChild = childOf(legacyParent);
    assert.equal(legacyChild.owner, "Luke");
    assert.deepEqual(legacyChild.coOwners, []);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("client commands carry the session's person, and others never look it up", () =>
  Effect.gen(function* () {
    const sessionId = AuthSessionId.make("session:people");
    let lookups = 0;
    const sessionsLabelled = (person: string | null) => ({
      getPerson: () => Effect.sync(() => (lookups++, person)),
    });
    const create: OrchestrationV2Command = {
      type: "thread.create",
      commandId: CommandId.make("stamp-create"),
      threadId: ThreadId.make("thread:stamp"),
      projectId: ProjectId.make("project:people"),
      title: "Stamped",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
      owner: "Luke",
    };
    const stampedCreate = yield* stampSessionPerson(sessionsLabelled("Pauli"), sessionId, create);
    assert.equal(stampedCreate.type === "thread.create" && stampedCreate.owner, "Pauli");
    const unlabelled = yield* stampSessionPerson(sessionsLabelled(null), sessionId, create);
    assert.equal(unlabelled.type === "thread.create" && unlabelled.owner, "Luke");

    const leave = yield* stampSessionPerson(sessionsLabelled("Pauli"), sessionId, {
      type: "thread.leave",
      commandId: CommandId.make("stamp-leave"),
      threadId: ThreadId.make("thread:stamp"),
      actor: "Luke",
    });
    assert.equal(leave.type === "thread.leave" && leave.actor, "Pauli");

    lookups = 0;
    const pin: OrchestrationV2Command = {
      type: "thread.pin",
      commandId: CommandId.make("stamp-pin"),
      threadId: ThreadId.make("thread:stamp"),
    };
    assert.deepEqual(yield* stampSessionPerson(sessionsLabelled("Pauli"), sessionId, pin), pin);
    assert.equal(lookups, 0);
  }),
);
