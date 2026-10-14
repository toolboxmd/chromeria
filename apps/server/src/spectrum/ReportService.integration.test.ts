import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";

import { ForkDispatchPlans } from "../fork/ForkDispatchPlans.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CommandReceipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import type { ProviderAdapterV2 } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Adapters from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Database from "../persistence/Sqlite.ts";
import * as Prism from "../prism/PrismService.ts";
import { errorFor } from "../prism/recovery.testkit.ts";
import * as History from "../prism/RecoveryHistory.ts";
import * as RecoveryStore from "../prism/RecoveryStore.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import { buildUnavailableProviderSnapshot } from "../provider/unavailableProviderSnapshot.ts";
import { reportsFence } from "../scheduledTaskChecks/handoff.ts";
import { bindSpectrum } from "../scheduledTaskChecks/spectra.testkit.ts";
import * as Settings from "../serverSettings.ts";
import * as Launch from "./LaunchService.ts";
import * as Report from "./ReportService.ts";
import {
  attemptOutcome,
  makeInitialReport,
  REPORT_EXHAUSTED,
  REPORT_NOT_RETRYABLE,
  REPORT_REJECTED,
  REPORT_STOPPED,
  reportAdmission,
  reportChainsUnchanged,
  reportDrainedPlan,
  reportDrainProof,
  reportUndeliveredGuard,
} from "./reportPolicy.ts";
import * as SchedulerAdapter from "./SchedulerAdapter.ts";
import { spectrumPlan } from "./spectrumPlan.ts";
import type { SpectrumState } from "./state.ts";
import { ensureSpectrumSchema, readSpectrum } from "./store.ts";
import { makeThread, NOW } from "./testFixtures.ts";

const instanceId = ProviderInstanceId.make("codex");
const selection = { instanceId, model: "test-model" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process runs in this test"),
} satisfies ProviderAdapterV2["Service"];
const adapters = Adapters.layerFromAdapters([adapter]);
const database = Database.layerMemory;
const registry = Layer.mock(Providers.ProviderRegistry)({
  getProviders: Effect.map(
    buildUnavailableProviderSnapshot({ driverKind: "codex", instanceId, reason: "fixture" }),
    (provider) => [
      {
        ...provider,
        enabled: true,
        installed: true,
        status: "ready" as const,
        availability: "available" as const,
        models: [{ slug: "test-model", name: "Test", isCustom: false, capabilities: null }],
      },
    ],
  ),
});
const base = Layer.mergeAll(
  database,
  adapters,
  registry,
  Prism.layer.pipe(Layer.provide(Layer.mergeAll(registry, Settings.layerTest({})))),
  CommandReceipts.layer.pipe(Layer.provide(database)),
  RecoveryStore.layer.pipe(Layer.provide(database)),
  Harness.layerWithRegistry({ name: "spectrum-report" }, adapters, {
    databaseLayer: database,
    runEffectWorker: false,
  }),
);
const runtime = Layer.fresh(
  Layer.mergeAll(Launch.layer, Report.layer, SchedulerAdapter.layer).pipe(Layer.provideMerge(base)),
);

const CALLER = ThreadId.make("caller");
const scheduled = (name: string) => `scheduler:${name}`;

/** A caller thread; `provider` names an instance no adapter serves, so sends to it are refused. */
const createCaller = (id: ThreadId, provider?: string) =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const thread = makeThread();
    const instance = provider === undefined ? instanceId : ProviderInstanceId.make(provider);
    yield* sink.write({
      events: [
        {
          id: EventId.make(`${id}:create`),
          type: "thread.created",
          threadId: id,
          occurredAt: NOW,
          payload: {
            ...thread,
            id,
            owner: "alice",
            providerInstanceId: instance,
            modelSelection: { ...thread.modelSelection, instanceId: instance },
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
          },
        },
      ],
    });
    return thread;
  });

const setup = Effect.gen(function* () {
  yield* ensureSpectrumSchema;
  yield* createCaller(CALLER);
});

/** A registered Spectrum, bound to its own scheduler run as registration binds a scheduled caller's. */
const start = (name: string, callerThreadId = CALLER) =>
  Effect.gen(function* () {
    const launch = yield* Launch.SpectrumLaunchService;
    const state = yield* launch.register({
      commandId: CommandId.make(`register:${name}`),
      threadId: ThreadId.make(`spectrum:${name}`),
      callerThreadId,
      callerRunId: null,
      question: "How should we build?",
      mode: "council",
      limit: 2,
      moderator: 1,
      colors: [
        { label: "Blue", selection },
        { label: "Red", selection },
      ],
    });
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE fork_spectra SET scheduler_run_id = ${scheduled(name)},
      payload_json = json_set(payload_json, '$.schedulerRunId', ${scheduled(name)})
      WHERE thread_id = ${state.threadId}`;
    return state.threadId;
  });

const read = (threadId: ThreadId) => readSpectrum(threadId).pipe(Effect.map(Option.getOrThrow));
const runsFor = (messageId: MessageId, callerThreadId = CALLER) =>
  Effect.gen(function* () {
    const projections = yield* Projection.ProjectionStoreV2;
    const { runs } = yield* projections.getThreadRecords(callerThreadId, ["runs"]);
    return runs.filter((run) => run.userMessageId === messageId);
  });
const runFor = (messageId: MessageId) =>
  runsFor(messageId).pipe(
    Effect.map((runs) => {
      assert.strictEqual(runs.length, 1);
      return runs[0]!;
    }),
  );

let fixtures = 0;
const commit = (events: ReadonlyArray<OrchestrationV2DomainEvent>, commandId?: string) =>
  Effect.gen(function* () {
    fixtures += 1;
    yield* (yield* EventSink.EventSinkV2).commitCommand({
      commandId: CommandId.make(commandId ?? `fixture:${fixtures}`),
      threadId: CALLER,
      commandType: "fixture",
      acceptedAt: NOW,
      events,
      effects: [],
    });
  });
const runEvent = (
  type: "run.created" | "run.updated",
  payload: OrchestrationV2Run,
): OrchestrationV2DomainEvent => {
  fixtures += 1;
  return {
    id: EventId.make(`event:${payload.id}:${fixtures}`),
    type,
    threadId: payload.threadId,
    occurredAt: NOW,
    payload,
  };
};
const ended = (run: OrchestrationV2Run, status: OrchestrationV2Run["status"]) =>
  runEvent("run.updated", { ...run, status, completedAt: NOW });
const itemEvent = (payload: OrchestrationV2TurnItem): OrchestrationV2DomainEvent => ({
  id: EventId.make(`event:${payload.id}`),
  type: "turn-item.updated",
  threadId: payload.threadId,
  occurredAt: NOW,
  payload,
});
const failure = (run: OrchestrationV2Run, retryable: boolean) => {
  const item = errorFor(run);
  if (item.type !== "error") throw new Error("errorFor returns an error item");
  return itemEvent({
    ...item,
    failure: { class: "provider_error", message: "Provider failed", code: null, retryable },
  });
};
const stopRequest = (run: OrchestrationV2Run) =>
  itemEvent({
    id: TurnItemId.make(`stop:${run.id}`),
    type: "run_interrupt_request",
    threadId: run.threadId,
    runId: run.id,
    nodeId: run.rootNodeId,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 2,
    status: "completed",
    title: "Interrupt requested",
    startedAt: NOW,
    completedAt: NOW,
    updatedAt: NOW,
    message: "Interrupt requested",
  });
/** Fails a report run with a typed failure, then records #169's conclusion for it. */
const fail = (
  run: OrchestrationV2Run,
  input: {
    readonly retryable: boolean;
    readonly reason: "non_mcp" | "exhausted" | "non_retryable";
  },
) =>
  Effect.gen(function* () {
    yield* commit([ended(run, "failed"), failure(run, input.retryable)]);
    yield* History.writeRecoveryOutcome({
      sourceRunId: run.id,
      threadId: run.threadId,
      status: "decided",
      outcome: "not_retryable",
      reason: input.reason,
    });
  });
const needsYou = (name: string) =>
  SchedulerAdapter.boundTo(scheduled(name)).pipe(Effect.map((rows) => rows[0]!.reportNeedsYou));

it.effect("a disabled caller holds the durable report without a run, effect or false receipt", () =>
  Effect.gen(function* () {
    yield* setup;
    const threadId = yield* start("disabled-caller");
    const reports = yield* Report.SpectrumReportService;
    const before = yield* reports.settle(
      threadId,
      "Report retained until the instance is enabled.",
    );
    const projections = yield* Projection.ProjectionStoreV2;
    const projectId = (yield* projections.getThread(CALLER)).projectId;
    const gate = Layer.fresh(Threads.layer).pipe(
      Layer.provideMerge(
        Settings.layerTest({
          providerInstances: {
            [ProviderInstanceId.make("codex")]: {
              driver: ProviderDriverKind.make("codex"),
              enabled: true,
            },
          },
          projectSettingsOverrides: {
            [projectId]: {
              providerInstances: { [ProviderInstanceId.make("codex")]: { enabled: false } },
            },
          },
        }),
      ),
    );
    const refused = yield* Effect.gen(function* () {
      return yield* (yield* Report.SpectrumReportService).deliver(threadId);
    }).pipe(Effect.provide(Report.layer.pipe(Layer.provide(gate))), Effect.flip);
    assert.instanceOf(refused.cause, Threads.ThreadManagementProviderInstanceDisabledError);
    assert.deepStrictEqual(yield* read(threadId), before);
    assert.deepStrictEqual(yield* runsFor(before.report!.messageId), []);
    const sql = yield* SqlClient.SqlClient;
    assert.deepStrictEqual(
      yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${CALLER}`,
      [],
    );
    assert.isTrue(
      Option.isNone(
        yield* (yield* CommandReceipts.CommandReceiptStoreV2).getByCommandId(
          before.report!.commandId,
        ),
      ),
    );
    assert.deepStrictEqual(yield* reportsFence(scheduled("disabled-caller")), { kind: "waiting" });
    assert.strictEqual(yield* needsYou("disabled-caller"), null);
    const delivered = yield* reports.deliver(threadId);
    assert.deepStrictEqual(delivered.report, before.report);
    assert.deepStrictEqual(delivered.outbox, []);
    assert.strictEqual(
      (yield* runFor(before.report!.messageId)).userMessageId,
      before.report!.messageId,
    );
  }).pipe(Effect.provide(runtime)),
);

it.effect(
  "an earlier report cannot be replaced if its effective completion changes before commit",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const threadId = yield* start("replace-race");
      const reports = yield* Report.SpectrumReportService;
      yield* reports.settle(threadId, "Earlier report.");
      const sent = yield* reports.deliver(threadId);
      const source = yield* runFor(sent.report!.messageId);
      yield* commit([ended(source, "completed")]);
      const sink = yield* EventSink.EventSinkV2;
      yield* sink.commitCommand({
        commandId: CommandId.make("replace-race:cycle"),
        threadId,
        commandType: "fixture",
        acceptedAt: NOW,
        events: [],
        effects: [],
        forkPlans: [
          spectrumPlan({
            expectedRevision: sent.revision,
            expectedGeneration: sent.generation,
            state: { ...sent, revision: sent.revision + 1, cycle: sent.cycle + 1 },
          }),
        ],
      });
      const before = yield* read(threadId);
      const refused = yield* Effect.gen(function* () {
        return yield* (yield* Report.SpectrumReportService).settle(threadId, "New cycle's report.");
      }).pipe(
        Effect.provide(Report.layer),
        Effect.provideService(EventSink.EventSinkV2, {
          ...sink,
          commitCommand: (command) =>
            command.commandType === "spectrum.report.settle"
              ? commit([
                  runEvent("run.updated", {
                    ...source,
                    status: "completed",
                    completedAt: NOW,
                    restartCancelledBackgroundWork: [
                      { kind: "task", id: "lost", label: "Awaited task" },
                    ],
                  }),
                ]).pipe(Effect.andThen(sink.commitCommand(command)))
              : sink.commitCommand(command),
        }),
        Effect.flip,
      );
      assert.strictEqual(
        (refused.cause as { readonly _tag?: string })._tag,
        "ForkCommitGuardRejected",
      );
      assert.deepStrictEqual(yield* read(threadId), before);
      assert.isTrue(
        Option.isNone(
          yield* (yield* CommandReceipts.CommandReceiptStoreV2).getByCommandId(
            CommandId.make(
              `spectrum:${threadId}:${before.generation}:${before.cycle}:report:1:settle`,
            ),
          ),
        ),
      );
      assert.strictEqual((yield* attemptOutcome(CALLER, sent.report!)).kind, "needs-you");
    }).pipe(Effect.provide(runtime)),
);

it.effect(
  "an abandoned report's native source effect prevents clearing its otherwise settled successor",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const threadId = yield* start("native-source-drain");
      const reports = yield* Report.SpectrumReportService;
      yield* reports.settle(threadId, "Report waiting on native restart.");
      const sent = yield* reports.deliver(threadId);
      const source = yield* runFor(sent.report!.messageId);
      const sink = yield* EventSink.EventSinkV2;
      const commandId = CommandId.make("command:runtime-reconcile:native-source-drain");
      const effectId = `effect:restart-continuation:${source.id}`;
      yield* sink.commitCommand({
        commandId,
        threadId: CALLER,
        commandType: "fixture",
        acceptedAt: NOW,
        events: [
          runEvent("run.updated", {
            ...source,
            status: "completed",
            completedAt: NOW,
            restartCancelledBackgroundWork: [
              { kind: "subagent", id: "lost", label: "Awaited child" },
            ],
          }),
        ],
        effects: [
          {
            id: effectId,
            commandId,
            threadId: CALLER,
            request: { type: "provider-runtime.continue", sourceRunId: source.id },
          },
        ],
      });
      const abandoned = yield* reports.abandon(threadId, sent.report!.commandId, "alice");
      const successor = {
        ...source,
        id: RunId.make("run:native-source-drain:successor"),
        ordinal: source.ordinal + 1,
        userMessageId: MessageId.make("message:native-source-drain:successor"),
        restartContinuationOfRunId: source.id,
        status: "completed" as const,
        completedAt: NOW,
      };
      yield* commit([runEvent("run.created", successor)]);
      const records = yield* (yield* Projection.ProjectionStoreV2).getThreadRecords(CALLER, [
        "attempts",
      ]);
      yield* commit(
        records.attempts
          .filter((attempt) => attempt.runId === source.id)
          .map((attempt) => ({
            id: EventId.make(`${attempt.id}:completed`),
            type: "run-attempt.updated" as const,
            threadId: CALLER,
            occurredAt: NOW,
            payload: { ...attempt, status: "completed" as const, completedAt: NOW },
          })),
      );
      const sql = yield* SqlClient.SqlClient;
      // The original start is finished; only the exact source continuation effect is still owed.
      yield* sql`UPDATE orchestration_v2_effect_outbox SET status='succeeded'
      WHERE command_id=${sent.report!.commandId}`;
      assert.isNull(yield* reportDrainProof(abandoned));
      yield* sql`UPDATE orchestration_v2_effect_outbox SET status='succeeded' WHERE effect_id=${effectId}`;
      const proof = yield* reportDrainProof(abandoned);
      assert.isNotNull(proof);
      if (proof === null) return;
      yield* sink.commitCommand({
        commandId: CommandId.make("native-source-drain:reopen-guard"),
        threadId,
        commandType: "fixture",
        acceptedAt: NOW,
        events: [],
        effects: [],
        forkPlans: [reportDrainedPlan(proof)],
      });
    }).pipe(Effect.provide(runtime)),
);

it.effect(
  "sends the exact persisted report after a restart and releases the scheduler only on its completed turn",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const threadId = yield* start("delivered");
      const reports = yield* Report.SpectrumReportService;
      const settled = yield* reports.settle(threadId, "Final answer: build it.");
      const report = settled.report!;
      assert.strictEqual(settled.status, "settled");
      assert.strictEqual(report.commandId, `spectrum:${threadId}:0:0:report:1`);
      assert.strictEqual(report.threadId, CALLER);
      assert.deepStrictEqual(settled.outbox, [report]);
      assert.deepStrictEqual((yield* reports.settle(threadId, "Another text")).report, report);
      assert.deepStrictEqual(yield* reportsFence(scheduled("delivered")), { kind: "waiting" });

      // A fresh service holds nothing in memory: replay reads the persisted command alone.
      const sent = yield* Effect.gen(function* () {
        return yield* (yield* Report.SpectrumReportService).deliver(threadId);
      }).pipe(Effect.provide(Report.layer));
      assert.deepStrictEqual(sent.report, report);
      assert.deepStrictEqual(sent.outbox, []);
      const run = yield* runFor(report.messageId);
      yield* reports.deliver(threadId);
      assert.strictEqual((yield* runsFor(report.messageId)).length, 1);
      const projections = yield* Projection.ProjectionStoreV2;
      const { messages } = yield* projections.getThreadRecords(CALLER, ["messages"], {
        messageIds: [report.messageId],
      });
      assert.strictEqual(messages[0]!.text, "Final answer: build it.");
      assert.deepStrictEqual(yield* reportsFence(scheduled("delivered")), { kind: "waiting" });

      yield* commit([ended(run, "completed")]);
      assert.deepStrictEqual(yield* reportsFence(scheduled("delivered")), { kind: "released" });
      assert.deepStrictEqual((yield* reports.deliver(threadId)).report, report);
      const refused = yield* reports.abandon(threadId, report.commandId, "alice").pipe(Effect.flip);
      assert.strictEqual(refused.cause, "The Spectrum report was already delivered");
    }).pipe(Effect.provide(runtime)),
);

it.effect(
  "retries only typed retryable failures after recovery concludes, three attempts in total",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const threadId = yield* start("retry");
      const reports = yield* Report.SpectrumReportService;
      yield* reports.settle(threadId, "Report text");
      const first = (yield* reports.deliver(threadId)).report!;
      const firstRun = yield* runFor(first.messageId);
      yield* commit([ended(firstRun, "failed"), failure(firstRun, true)]);
      // No recovery record yet, then a pending one: neither concludes the attempt.
      assert.deepStrictEqual((yield* reports.deliver(threadId)).report, first);
      yield* History.writeRecoveryOutcome({
        sourceRunId: firstRun.id,
        threadId: CALLER,
        status: "pending",
        reason: "decision",
      });
      assert.deepStrictEqual((yield* reports.deliver(threadId)).report, first);
      assert.strictEqual(yield* needsYou("retry"), null);
      yield* History.writeRecoveryOutcome({
        sourceRunId: firstRun.id,
        threadId: CALLER,
        status: "decided",
        outcome: "not_retryable",
        reason: "non_mcp",
      });

      const second = (yield* reports.deliver(threadId)).report!;
      assert.strictEqual(second.commandId, `spectrum:${threadId}:0:0:report:2`);
      assert.strictEqual(second.messageId, `spectrum:${threadId}:0:0:report:2:message`);
      assert.strictEqual(second.text, first.text);
      yield* fail(yield* runFor(second.messageId), { retryable: true, reason: "exhausted" });
      const third = (yield* reports.deliver(threadId)).report!;
      assert.strictEqual(third.commandId, `spectrum:${threadId}:0:0:report:3`);
      const thirdRun = yield* runFor(third.messageId);
      yield* fail(thirdRun, { retryable: true, reason: "non_mcp" });

      const held = yield* reports.deliver(threadId);
      assert.deepStrictEqual(held.report, third);
      assert.deepStrictEqual(yield* needsYou("retry"), {
        commandId: third.commandId,
        reason: REPORT_EXHAUSTED,
      });
      assert.deepStrictEqual(yield* reportsFence(scheduled("retry")), {
        kind: "needs-you",
        reason: REPORT_EXHAUSTED,
      });

      // Captured evidence is rechecked in the commit: a new continuation rejects it.
      const outcome = yield* attemptOutcome(CALLER, third);
      assert.strictEqual(outcome.kind, "retryable");
      if (outcome.kind !== "retryable") return;
      yield* commit([
        runEvent("run.created", {
          ...thirdRun,
          id: RunId.make("run:retry:continuation"),
          ordinal: thirdRun.ordinal + 1,
          userMessageId: MessageId.make("message:retry:continuation"),
          restartContinuationOfRunId: thirdRun.id,
          status: "running",
          completedAt: null,
        }),
      ]);
      const sink = yield* EventSink.EventSinkV2;
      const rejected = yield* sink
        .commitCommand({
          commandId: CommandId.make("stale:evidence"),
          threadId,
          commandType: "fixture",
          acceptedAt: NOW,
          events: [],
          effects: [],
          forkPlans: [reportChainsUnchanged(threadId, [outcome.chain])],
        })
        .pipe(Effect.flip);
      assert.strictEqual(rejected._tag, "ForkCommitGuardRejected");
      assert.strictEqual(yield* needsYou("retry"), null);
    }).pipe(Effect.provide(runtime)),
);

it.effect("never retries an explicit Stop, a non-retryable failure or a rejected send", () =>
  Effect.gen(function* () {
    yield* setup;
    const reports = yield* Report.SpectrumReportService;

    const stopped = yield* start("stop");
    yield* reports.settle(stopped, "Stopped report");
    const stopReport = (yield* reports.deliver(stopped)).report!;
    const stopRun = yield* runFor(stopReport.messageId);
    yield* commit([stopRequest(stopRun), ended(stopRun, "interrupted")]);
    assert.deepStrictEqual((yield* reports.deliver(stopped)).report, stopReport);
    assert.deepStrictEqual(yield* needsYou("stop"), {
      commandId: stopReport.commandId,
      reason: REPORT_STOPPED,
    });
    assert.deepStrictEqual(yield* reportsFence(scheduled("stop")), {
      kind: "needs-you",
      reason: REPORT_STOPPED,
    });

    const fatal = yield* start("fatal");
    yield* reports.settle(fatal, "Fatal report");
    const fatalReport = (yield* reports.deliver(fatal)).report!;
    yield* fail(yield* runFor(fatalReport.messageId), { retryable: false, reason: "non_mcp" });
    assert.deepStrictEqual((yield* reports.deliver(fatal)).report, fatalReport);
    assert.deepStrictEqual(yield* needsYou("fatal"), {
      commandId: fatalReport.commandId,
      reason: REPORT_NOT_RETRYABLE,
    });

    // The recipient refuses the send while planning it: a durable rejected receipt ends the attempt.
    const ghost = ThreadId.make("caller:ghost");
    yield* createCaller(ghost, "ghost");
    const refused = yield* start("refused", ghost);
    const refusedReport = (yield* reports.settle(refused, "Refused report")).report!;
    // An enabled instance with no adapter reaches the refusal seam, rather than the earlier off gate.
    const ghostGate = Layer.fresh(Threads.layer).pipe(
      Layer.provide(
        Settings.layerTest({
          providerInstances: {
            [ProviderInstanceId.make("ghost")]: {
              driver: ProviderDriverKind.make("codex"),
              enabled: true,
            },
          },
        }),
      ),
    );
    const afterRejection = yield* Effect.gen(function* () {
      return yield* (yield* Report.SpectrumReportService).deliver(refused);
    }).pipe(Effect.provide(Report.layer.pipe(Layer.provide(ghostGate))));
    const receipts = yield* CommandReceipts.CommandReceiptStoreV2;
    assert.strictEqual(
      Option.getOrThrow(yield* receipts.getByCommandId(refusedReport.commandId)).status,
      "rejected",
    );
    assert.deepStrictEqual(afterRejection.outbox, []);
    assert.deepStrictEqual(afterRejection.report, refusedReport);
    assert.deepStrictEqual(yield* runsFor(refusedReport.messageId, ghost), []);
    assert.deepStrictEqual((yield* reports.deliver(refused)).report, refusedReport);
    assert.deepStrictEqual(yield* needsYou("refused"), {
      commandId: refusedReport.commandId,
      reason: REPORT_REJECTED,
    });

    // A commit guard that rolls back leaves no receipt: the attempt stays unsent and holds.
    const retiredCaller = ThreadId.make("caller:retired");
    const retiredThread = yield* createCaller(retiredCaller);
    const held = yield* start("held", retiredCaller);
    const heldReport = (yield* reports.settle(held, "Held report")).report!;
    yield* (yield* EventSink.EventSinkV2).write({
      events: [
        {
          id: EventId.make("caller:retired:stop"),
          type: "thread.metadata-updated",
          threadId: retiredCaller,
          occurredAt: NOW,
          payload: {
            ...retiredThread,
            id: retiredCaller,
            owner: "alice",
            lineage: {
              parentThreadId: null,
              relationshipToParent: null,
              rootThreadId: retiredCaller,
            },
            forkRetirement: { token: CommandId.make("caller:retired:stop") },
          },
        },
      ],
    });
    yield* reports.deliver(held).pipe(Effect.flip);
    assert.deepStrictEqual((yield* read(held)).outbox, [heldReport]);
    assert.isTrue(Option.isNone(yield* receipts.getByCommandId(heldReport.commandId)));
    assert.strictEqual(yield* needsYou("held"), null);
    assert.deepStrictEqual(yield* reportsFence(scheduled("held")), { kind: "waiting" });
  }).pipe(Effect.provide(runtime)),
);

it.effect(
  "a restart cancellation holds until its exact continuation completes; another cancellation is Stop",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const reports = yield* Report.SpectrumReportService;
      const threadId = yield* start("restart");
      yield* reports.settle(threadId, "Restarted report");
      const report = (yield* reports.deliver(threadId)).report!;
      const run = yield* runFor(report.messageId);
      yield* commit([ended(run, "cancelled")], `command:runtime-reconcile:startup:${CALLER}:1`);
      assert.deepStrictEqual((yield* reports.deliver(threadId)).report, report);
      assert.strictEqual(yield* needsYou("restart"), null);
      assert.deepStrictEqual(yield* reportsFence(scheduled("restart")), { kind: "waiting" });

      yield* commit([
        runEvent("run.created", {
          ...run,
          id: RunId.make("run:restart:continuation"),
          ordinal: run.ordinal + 1,
          userMessageId: MessageId.make("message:restart-continuation"),
          restartContinuationOfRunId: run.id,
          status: "completed",
          completedAt: NOW,
        }),
      ]);
      assert.strictEqual((yield* attemptOutcome(CALLER, report)).kind, "delivered");
      assert.deepStrictEqual(yield* reportsFence(scheduled("restart")), { kind: "released" });

      const other = yield* start("cancelled");
      yield* reports.settle(other, "Cancelled report");
      const cancelled = (yield* reports.deliver(other)).report!;
      yield* commit(
        [ended(yield* runFor(cancelled.messageId), "cancelled")],
        "fixture:user-cancel",
      );
      assert.deepStrictEqual((yield* reports.deliver(other)).report, cancelled);
      assert.deepStrictEqual(yield* needsYou("cancelled"), {
        commandId: cancelled.commandId,
        reason: REPORT_STOPPED,
      });
    }).pipe(Effect.provide(runtime)),
);

it.effect(
  "human abandonment cancels the queued report, refuses delayed sends and queue release, then admits reopen",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const reports = yield* Report.SpectrumReportService;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      // The caller is busy, so the report queues behind its current turn.
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("caller:work"),
        messageId: MessageId.make("caller:work"),
        threadId: CALLER,
        text: "Keep working",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "server",
      });
      const queued = yield* start("queued");
      yield* reports.settle(queued, "Queued report");
      const report = (yield* reports.deliver(queued)).report!;
      assert.strictEqual((yield* runFor(report.messageId)).status, "queued");

      const wrong = yield* reports
        .abandon(queued, CommandId.make("report:other"), "alice")
        .pipe(Effect.flip);
      assert.strictEqual(wrong.cause, "Only the current Spectrum report can be abandoned");
      assert.strictEqual(
        (yield* reports.abandon(queued, report.commandId, " ").pipe(Effect.flip)).cause,
        "A person must abandon the Spectrum report",
      );
      const abandoned = yield* reports.abandon(queued, report.commandId, "alice");
      assert.strictEqual(abandoned.reportAbandonment?.commandId, report.commandId);
      assert.strictEqual(abandoned.reportAbandonment?.person, "alice");
      assert.deepStrictEqual(abandoned.report, report);
      assert.strictEqual((yield* runFor(report.messageId)).status, "cancelled");
      assert.deepStrictEqual(yield* reportsFence(scheduled("queued")), { kind: "released" });

      // A send that read the outbox before abandonment commits after it: admission refuses it.
      const unsent = yield* start("unsent");
      const pending = (yield* reports.settle(unsent, "Unsent report")).report!;
      yield* reports.abandon(unsent, pending.commandId, "alice");
      yield* orchestrator
        .dispatch(pending)
        .pipe(
          Effect.provideService(ForkDispatchPlans, [reportAdmission(unsent, pending)]),
          Effect.flip,
        );
      assert.deepStrictEqual(yield* runsFor(pending.messageId), []);

      // Releasing the queue after the caller's turn starts nothing abandoned.
      const work = yield* runFor(MessageId.make("caller:work"));
      yield* commit([ended(work, "completed")]);
      yield* orchestrator.dispatch({
        type: "queue.resume",
        commandId: CommandId.make("caller:resume"),
        threadId: CALLER,
      });
      assert.deepStrictEqual(
        (yield* runsFor(report.messageId)).map((run) => run.status),
        ["cancelled"],
      );

      const before = yield* read(queued);
      const proof = yield* reportDrainProof(before);
      assert.isNotNull(proof);
      if (proof === null) return;
      const sink = yield* EventSink.EventSinkV2;
      yield* sink.commitCommand({
        commandId: CommandId.make("reopen:queued"),
        threadId: queued,
        commandType: "fixture",
        acceptedAt: NOW,
        events: [],
        effects: [],
        forkPlans: [
          spectrumPlan({
            expectedRevision: before.revision,
            expectedGeneration: before.generation,
            state: {
              ...before,
              revision: before.revision + 1,
              status: "active",
              cycle: before.cycle + 1,
              report: null,
              reportAbandonment: null,
            },
          }),
          reportDrainedPlan(proof),
        ],
      });
      const reopened = yield* reports.deliver(queued);
      assert.strictEqual(reopened.report, null);
      assert.strictEqual(reopened.status, "active");
      // A replayed old send finds its first receipt; it starts nothing new.
      yield* orchestrator
        .dispatch(report)
        .pipe(
          Effect.provideService(ForkDispatchPlans, [reportAdmission(queued, report)]),
          Effect.ignore,
        );
      assert.deepStrictEqual(
        (yield* runsFor(report.messageId)).map((run) => run.status),
        ["cancelled"],
      );
    }).pipe(Effect.provide(runtime)),
);

it.effect("a completed report with lost awaited work holds until its exact restart completes", () =>
  Effect.gen(function* () {
    yield* setup;
    const reports = yield* Report.SpectrumReportService;
    const threadId = yield* start("lost-work");
    yield* reports.settle(threadId, "Report waiting for background work");
    const report = (yield* reports.deliver(threadId)).report!;
    const source = yield* runFor(report.messageId);
    const sink = yield* EventSink.EventSinkV2;
    const commandId = CommandId.make(`command:runtime-reconcile:startup:${CALLER}:lost-work`);
    yield* sink.commitCommand({
      commandId,
      threadId: CALLER,
      commandType: "fixture",
      acceptedAt: NOW,
      events: [
        runEvent("run.updated", {
          ...source,
          status: "completed",
          completedAt: NOW,
          restartCancelledBackgroundWork: [{ kind: "monitor", id: "lost", label: "Awaited work" }],
        }),
      ],
      effects: [
        {
          id: `effect:restart-continuation:${source.id}`,
          commandId,
          threadId: CALLER,
          request: { type: "provider-runtime.continue", sourceRunId: source.id },
        },
      ],
    });
    assert.strictEqual((yield* attemptOutcome(CALLER, report)).kind, "held");
    assert.deepStrictEqual((yield* reports.deliver(threadId)).report, report);
    assert.strictEqual(yield* needsYou("lost-work"), null);
    assert.deepStrictEqual(yield* reportsFence(scheduled("lost-work")), { kind: "waiting" });

    // Admission writes the successor before the continuation effect settles.
    const successor = {
      ...source,
      id: RunId.make("run:lost-work:successor"),
      ordinal: source.ordinal + 1,
      userMessageId: MessageId.make("message:lost-work:successor"),
      restartContinuationOfRunId: source.id,
      status: "running" as const,
      completedAt: null,
    };
    yield* commit([runEvent("run.created", successor)]);
    assert.strictEqual((yield* attemptOutcome(CALLER, report)).kind, "open");
    assert.deepStrictEqual(yield* reportsFence(scheduled("lost-work")), { kind: "waiting" });
    yield* commit([ended(successor, "completed")]);
    assert.strictEqual((yield* attemptOutcome(CALLER, report)).kind, "delivered");
    assert.deepStrictEqual(yield* reportsFence(scheduled("lost-work")), { kind: "released" });
    assert.strictEqual(
      (yield* reports.abandon(threadId, report.commandId, "alice").pipe(Effect.flip)).cause,
      "The Spectrum report was already delivered",
    );
  }).pipe(Effect.provide(runtime)),
);

it.effect.each(["completed", "cancelled"] as const)(
  "a %s report whose restart declines needs the user and can be abandoned without another attempt",
  (status) =>
    Effect.gen(function* () {
      yield* setup;
      const reports = yield* Report.SpectrumReportService;
      const name = `restart-declined:${status}`;
      const threadId = yield* start(name);
      yield* reports.settle(threadId, "Report with cancelled awaited work");
      const report = (yield* reports.deliver(threadId)).report!;
      const source = yield* runFor(report.messageId);
      const sink = yield* EventSink.EventSinkV2;
      const commandId = CommandId.make(`command:runtime-reconcile:startup:${CALLER}:${status}`);
      const effectId = `effect:restart-continuation:${source.id}`;
      yield* sink.commitCommand({
        commandId,
        threadId: CALLER,
        commandType: "fixture",
        acceptedAt: NOW,
        events: [
          runEvent("run.updated", {
            ...source,
            status,
            completedAt: NOW,
            restartCancelledBackgroundWork: [{ kind: "task", id: "lost", label: "Awaited task" }],
          }),
        ],
        effects: [
          {
            id: effectId,
            commandId,
            threadId: CALLER,
            request: { type: "provider-runtime.continue", sourceRunId: source.id },
          },
        ],
      });
      assert.strictEqual((yield* attemptOutcome(CALLER, report)).kind, "held");
      // Native continuation can succeed by declining, without a new run or event.
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE orchestration_v2_effect_outbox SET status = 'succeeded'
        WHERE effect_id = ${effectId}`;
      const outcome = yield* attemptOutcome(CALLER, report);
      assert.strictEqual(outcome.kind, "needs-you");
      assert.deepStrictEqual(yield* needsYou(name), {
        commandId: report.commandId,
        reason: REPORT_NOT_RETRYABLE,
      });
      assert.deepStrictEqual(yield* reportsFence(scheduled(name)), {
        kind: "needs-you",
        reason: REPORT_NOT_RETRYABLE,
      });
      assert.deepStrictEqual((yield* reports.deliver(threadId)).report, report);
      const abandoned = yield* reports.abandon(threadId, report.commandId, "alice");
      assert.strictEqual(abandoned.reportAbandonment?.commandId, report.commandId);
      assert.deepStrictEqual(yield* reportsFence(scheduled(name)), { kind: "released" });
    }).pipe(Effect.provide(runtime)),
);

it.effect(
  "a report chain guard rejects newly lost awaited work even when raw status is unchanged",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const reports = yield* Report.SpectrumReportService;
      const threadId = yield* start("loss-race");
      yield* reports.settle(threadId, "Report that appeared complete");
      const report = (yield* reports.deliver(threadId)).report!;
      const source = yield* runFor(report.messageId);
      yield* commit([ended(source, "completed")]);
      const outcome = yield* attemptOutcome(CALLER, report);
      assert.strictEqual(outcome.kind, "delivered");
      if (outcome.kind !== "delivered") return;
      yield* commit([
        {
          id: EventId.make("event:loss-race:background-work"),
          type: "run.background-work-cancelled",
          threadId: CALLER,
          runId: source.id,
          providerInstanceId: source.providerInstanceId,
          occurredAt: NOW,
          payload: {
            runId: source.id,
            restartCancelledBackgroundWork: [
              { kind: "subagent", id: "lost", label: "Awaited child" },
            ],
          },
        },
      ]);
      assert.strictEqual((yield* runFor(report.messageId)).status, "completed");
      const rejected = yield* (yield* EventSink.EventSinkV2)
        .commitCommand({
          commandId: CommandId.make("stale:loss-race:evidence"),
          threadId,
          commandType: "fixture",
          acceptedAt: NOW,
          events: [],
          effects: [],
          forkPlans: [reportChainsUnchanged(threadId, [outcome.chain])],
        })
        .pipe(Effect.flip);
      assert.strictEqual(rejected._tag, "ForkCommitGuardRejected");
      // A lost-work source never blocks the human's abandonment as a delivered turn.
      const abandoned = yield* reports.abandon(threadId, report.commandId, "alice");
      assert.strictEqual(abandoned.reportAbandonment?.commandId, report.commandId);
    }).pipe(Effect.provide(runtime)),
);

it.effect("a report turn that completes between abandon's read and its commit refuses it", () =>
  Effect.gen(function* () {
    yield* setup;
    const reports = yield* Report.SpectrumReportService;
    const sink = yield* EventSink.EventSinkV2;
    const receipts = yield* CommandReceipts.CommandReceiptStoreV2;
    /** Runs abandon on a fresh service whose sink commits `completion` just before the abandonment. */
    const abandonRacing = (
      threadId: ThreadId,
      commandId: CommandId,
      completion: ReadonlyArray<OrchestrationV2DomainEvent>,
    ) =>
      Effect.gen(function* () {
        return yield* (yield* Report.SpectrumReportService).abandon(threadId, commandId, "alice");
      }).pipe(
        Effect.provide(Report.layer),
        Effect.provideService(
          EventSink.EventSinkV2,
          EventSink.EventSinkV2.of({
            ...sink,
            commitCommand: (input) =>
              input.commandType === "spectrum.report.abandon"
                ? sink
                    .commitCommand({
                      commandId: CommandId.make(`race:${threadId}`),
                      threadId: CALLER,
                      commandType: "fixture",
                      acceptedAt: NOW,
                      events: completion,
                      effects: [],
                    })
                    .pipe(Effect.orDie, Effect.andThen(sink.commitCommand(input)))
                : sink.commitCommand(input),
          }),
        ),
        Effect.flip,
      );
    const refusedUnchanged = (
      threadId: ThreadId,
      before: SpectrumState,
      refused: Report.SpectrumReportError,
    ) =>
      Effect.gen(function* () {
        assert.strictEqual(
          (refused.cause as { readonly _tag?: string })._tag,
          "ForkCommitGuardRejected",
        );
        assert.deepStrictEqual(yield* read(threadId), before);
        const abandonReceipt = yield* receipts.getByCommandId(
          CommandId.make(`${before.report!.commandId}:abandon`),
        );
        assert.isTrue(Option.isNone(abandonReceipt));
      });

    const direct = yield* start("race");
    yield* reports.settle(direct, "Racing report");
    const before = yield* reports.deliver(direct);
    const report = before.report!;
    const run = yield* runFor(report.messageId);
    // While the turn is open, the same guard admits.
    yield* sink.commitCommand({
      commandId: CommandId.make("race:open"),
      threadId: direct,
      commandType: "fixture",
      acceptedAt: NOW,
      events: [],
      effects: [],
      forkPlans: [reportUndeliveredGuard(direct, report)],
    });
    const refused = yield* abandonRacing(direct, report.commandId, [ended(run, "completed")]);
    yield* refusedUnchanged(direct, before, refused);
    assert.strictEqual((yield* runFor(report.messageId)).status, "completed");
    assert.deepStrictEqual(yield* reportsFence(scheduled("race")), { kind: "released" });

    // Delivery through a restart continuation is found on the recorded chain.
    const chained = yield* start("race-chain");
    yield* reports.settle(chained, "Continued report");
    const beforeChain = yield* reports.deliver(chained);
    const source = yield* runFor(beforeChain.report!.messageId);
    yield* commit([ended(source, "cancelled")], `command:runtime-reconcile:startup:${CALLER}:race`);
    const refusedChain = yield* abandonRacing(chained, beforeChain.report!.commandId, [
      runEvent("run.created", {
        ...source,
        id: RunId.make("run:race:continuation"),
        ordinal: source.ordinal + 1,
        userMessageId: MessageId.make("message:race-continuation"),
        restartContinuationOfRunId: source.id,
        status: "completed",
        completedAt: NOW,
      }),
    ]);
    yield* refusedUnchanged(chained, beforeChain, refusedChain);
  }).pipe(Effect.provide(runtime)),
);

it.effect(
  "a Stop notice still sends after the transcript retired and a retired Spectrum never retries",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const reports = yield* Report.SpectrumReportService;
      const threadId = yield* start("notice");
      const sink = yield* EventSink.EventSinkV2;
      const before = yield* read(threadId);
      // The controller's Stop plan: a new generation, retired, with its notice as the first attempt.
      const stopped = makeInitialReport(
        { ...before, generation: before.generation + 1, revision: before.revision + 1 },
        "Spectrum stopped: the user asked.",
        true,
      );
      yield* sink.commitCommand({
        commandId: CommandId.make("stop:notice"),
        threadId,
        commandType: "fixture",
        acceptedAt: NOW,
        events: [],
        effects: [],
        forkPlans: [
          spectrumPlan({
            expectedRevision: before.revision,
            expectedGeneration: before.generation,
            state: stopped,
          }),
        ],
      });
      const projections = yield* Projection.ProjectionStoreV2;
      const shell = yield* projections.getThread(threadId);
      yield* sink.write({
        events: [
          {
            id: EventId.make("notice:retire"),
            type: "thread.metadata-updated",
            threadId,
            occurredAt: NOW,
            payload: { ...shell, forkRetirement: { token: CommandId.make("stop:notice") } },
          },
        ],
      });
      const sent = yield* reports.deliver(threadId);
      const notice = sent.report!;
      assert.strictEqual(notice.commandId, `spectrum:${threadId}:1:0:report:1`);
      assert.deepStrictEqual(sent.outbox, []);
      const run = yield* runFor(notice.messageId);
      yield* fail(run, { retryable: true, reason: "non_mcp" });
      assert.deepStrictEqual((yield* reports.deliver(threadId)).report, notice);
      assert.deepStrictEqual(yield* needsYou("notice"), {
        commandId: notice.commandId,
        reason: REPORT_STOPPED,
      });
      const abandoned = yield* reports.abandon(threadId, notice.commandId, "alice");
      assert.strictEqual(abandoned.reportAbandonment?.commandId, notice.commandId);
      assert.deepStrictEqual(yield* reportsFence(scheduled("notice")), { kind: "released" });
    }).pipe(Effect.provide(runtime)),
);

it.effect("holds an unknown report identity and an unreadable row for a person", () =>
  Effect.gen(function* () {
    yield* setup;
    const reports = yield* Report.SpectrumReportService;
    const legacy = yield* bindSpectrum({
      callerThreadId: CALLER,
      schedulerRunId: scheduled("legacy"),
      status: "settled",
      report: "legacy",
      inOutbox: true,
    });
    const held = yield* reports.deliver(ThreadId.make(legacy));
    assert.strictEqual(held.outbox.length, 1);
    assert.deepStrictEqual(yield* runsFor(MessageId.make("report-message:legacy")), []);
    assert.strictEqual(yield* needsYou("legacy"), null);
    assert.deepStrictEqual(yield* reportsFence(scheduled("legacy")), { kind: "waiting" });

    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO fork_spectra (thread_id, caller_thread_id, scheduler_run_id, generation, revision, payload_json)
      VALUES ('spectrum:broken', ${CALLER}, ${scheduled("broken")}, 0, 0, '{"version":1}')`;
    assert.deepStrictEqual(yield* SchedulerAdapter.boundTo(scheduled("broken")), [
      { status: "active", report: null, reportAbandonment: null, reportNeedsYou: null },
    ]);
    assert.deepStrictEqual(yield* reportsFence(scheduled("broken")), { kind: "waiting" });
  }).pipe(Effect.provide(runtime)),
);

it.effect(
  "a rename between lifecycle planning and settlement commits is preserved and settlement retries atomically",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const threadId = yield* start("rename-race");
      const before = yield* read(threadId);
      const projections = yield* Projection.ProjectionStoreV2;
      const thread = yield* projections.getThread(threadId);
      const sink = yield* EventSink.EventSinkV2;
      const refusal = yield* Effect.gen(function* () {
        return yield* (yield* Report.SpectrumReportService).settle(threadId, "Report after rename");
      }).pipe(
        Effect.provide(Report.layer),
        Effect.provideService(EventSink.EventSinkV2, {
          ...sink,
          commitCommand: (input) =>
            sink
              .write({
                events: [
                  {
                    id: EventId.make("concurrent:rename"),
                    type: "thread.metadata-updated",
                    threadId,
                    occurredAt: NOW,
                    payload: { ...thread, title: "The person's renamed council" },
                  },
                ],
              })
              .pipe(Effect.andThen(sink.commitCommand(input))),
        }),
        Effect.flip,
      );
      assert.strictEqual((refusal.cause as { _tag?: string })._tag, "ForkCommitGuardRejected");
      assert.deepStrictEqual(yield* read(threadId), before);
      assert.strictEqual(
        (yield* projections.getThread(threadId)).title,
        "The person's renamed council",
      );
      assert.strictEqual((yield* projections.getThreadShell(threadId))!.forkSpectrumRunning, true);
      const sql = yield* SqlClient.SqlClient;
      assert.deepStrictEqual(
        yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_type='spectrum.report.settle' AND aggregate_id=${threadId}`,
        [],
      );
      const settled = yield* (yield* Report.SpectrumReportService).settle(
        threadId,
        "Report after rename",
      );
      assert.strictEqual(settled.status, "settled");
      assert.strictEqual(
        (yield* projections.getThread(threadId)).title,
        "The person's renamed council",
      );
      assert.strictEqual((yield* projections.getThreadShell(threadId))!.forkSpectrumRunning, false);
    }).pipe(Effect.provide(runtime)),
);
