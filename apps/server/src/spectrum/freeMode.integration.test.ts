import * as BareLaunch from "../orchestration-v2/BareLaunch.ts";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Events from "../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import { layer as serialization } from "../orchestration-v2/ThreadCommandExecutor.ts";
import { initializeRecoveryHistory, writeRecoveryOutcome } from "../prism/RecoveryHistory.ts";
import * as Controller from "./Controller.ts";
import * as McpInterrupt from "./mcpInterrupt.ts";
import * as CommandPlans from "./commandPlan.ts";
import * as Adapter from "./SchedulerAdapter.ts";
import * as Launch from "./LaunchService.ts";
import * as Round from "./RoundService.ts";
import * as Transcript from "./TranscriptService.ts";
import * as Reports from "./ReportService.ts";
import * as Settings from "../serverSettings.ts";
import { base, input, setup, spareInstanceId } from "./controllerTestkit.ts";
import { NOW } from "./testFixtures.ts";
import { memberOf, type SpectrumState } from "./state.ts";
import { readSpectrum } from "./store.ts";

const dependencies = Layer.mergeAll(base, serialization);
const services = Layer.mergeAll(
  Launch.layer.pipe(Layer.provide(Layer.mock(BareLaunch.BareLaunch)({}))),
  Round.layer,
  Transcript.layer,
  Reports.layer,
).pipe(Layer.provideMerge(dependencies));
const runtime = Layer.fresh(
  Layer.mergeAll(
    CommandPlans.layer.pipe(Layer.provide(dependencies)),
    McpInterrupt.layer.pipe(Layer.provide(dependencies)),
    Adapter.layer,
    Controller.layer.pipe(Layer.provideMerge(services)),
  ),
);

const selection = { instanceId: ProviderInstanceId.make("codex"), model: "test-model" };
const free = (colors: ReadonlyArray<string>, limit: number): Launch.SpectrumStartRequest => ({
  ...input,
  mode: "free",
  limit,
  moderator: undefined,
  colors: colors.map((label) => ({ label, selection })),
});

/** Every resume runs on a fresh controller, as after a server restart. */
const resume = (threadId: ThreadId) =>
  Effect.gen(function* () {
    return yield* (yield* Controller.SpectrumController).resume(threadId);
  }).pipe(Effect.provide(Layer.fresh(Controller.layer)));

const currentRun = Effect.fn("test.free.currentRun")(function* (state: SpectrumState) {
  const slot = state.round!.slots[0]!;
  const records = yield* (yield* Projection.ProjectionStoreV2).getThreadRecords(slot.threadId, [
    "runs",
    "messages",
  ]);
  const run = records.runs.find((candidate) => candidate.id === slot.runId)!;
  const prompt = records.messages.find((message) => message.id === run.userMessageId)!.text;
  return {
    run,
    prompt,
    label: memberOf(state, slot.threadId)!.label,
  };
});

const end = Effect.fn("test.free.end")(function* (
  run: OrchestrationV2Run,
  outcome: { readonly text: string } | { readonly failed: true },
) {
  yield* (yield* Events.EventSinkV2).write({
    events: [
      {
        id: EventId.make(`${run.id}:end`),
        type: "run.updated",
        threadId: run.threadId,
        occurredAt: NOW,
        payload: { ...run, status: "text" in outcome ? "completed" : "failed", completedAt: NOW },
      },
      ...("text" in outcome
        ? [
            {
              id: EventId.make(`${run.id}:reply`),
              type: "message.updated" as const,
              threadId: run.threadId,
              occurredAt: NOW,
              payload: {
                id: MessageId.make(`${run.id}:reply`),
                threadId: run.threadId,
                runId: run.id,
                nodeId: null,
                createdBy: "agent" as const,
                creationSource: "provider" as const,
                role: "assistant" as const,
                text: outcome.text,
                attachments: [],
                streaming: false,
                createdAt: NOW,
                updatedAt: NOW,
              },
            },
          ]
        : []),
    ],
  });
  // Prism's recovery decides a failed run before a Spectrum may treat it as ended.
  if (!("text" in outcome))
    yield* writeRecoveryOutcome({
      sourceRunId: run.id,
      threadId: run.threadId,
      status: "decided",
      outcome: "not_retryable",
      reason: "non_retryable",
    });
});

/** Answers the current call and returns who spoke, in which phase, after which prompt. */
const answer = Effect.fn("test.free.answer")(function* (
  state: SpectrumState,
  outcome: { readonly text: string } | { readonly failed: true },
) {
  const { run, prompt, label } = yield* currentRun(state);
  const phase = state.round!.phase;
  yield* end(run, outcome);
  return { next: yield* resume(state.threadId), spoke: `${phase}:${label}`, prompt };
});

const transcriptTexts = Effect.fn("test.free.transcript")(function* (threadId: ThreadId) {
  const records = yield* (yield* Projection.ProjectionStoreV2).getThreadRecords(threadId, [
    "messages",
  ]);
  return records.messages.map((message) => message.text);
});

const boot = Effect.gen(function* () {
  yield* setup;
  yield* initializeRecoveryHistory;
});

const register = (start: Launch.SpectrumStartRequest) =>
  Effect.gen(function* () {
    const state = yield* (yield* Launch.SpectrumLaunchService).register(start);
    return yield* resume(state.threadId);
  });

it.effect("a question-only start uses configured White and cap throughout free moderation", () =>
  Effect.gen(function* () {
    yield* boot;
    yield* (yield* Settings.ServerSettingsService).updateSettings({
      spectrum: {
        slots: ["Blue", "Red"].map((label) => ({ label, primary: selection, fallbacks: [] })),
        white: { primary: { ...selection, instanceId: spareInstanceId }, fallbacks: [] },
        mode: "free",
        relayRounds: 2,
        turnsPerColor: 1,
      },
    });
    let state = yield* register({
      commandId: input.commandId,
      threadId: input.threadId,
      callerThreadId: input.callerThreadId,
      callerRunId: input.callerRunId,
      question: "Use the saved free discussion",
    });
    assert.strictEqual(state.limit, 2);
    assert.strictEqual(state.turnsPerColor, 1);
    // White is its own member on its own model, and never one of the Colors.
    assert.isUndefined(state.moderator);
    assert.strictEqual(state.white!.selection.instanceId, spareInstanceId);
    assert.deepStrictEqual(
      state.participants.map((color) => color.label),
      ["Blue", "Red"],
    );
    const spoken: string[] = [];
    for (const text of ["Blue opens.", "Red opens.", "DONE", "Configured White's summary."]) {
      const step = yield* answer(state, { text });
      spoken.push(step.spoke);
      if (step.spoke === "moderation:White") assert.include(step.prompt, "turn 2 of 2");
      state = step.next;
    }
    assert.deepStrictEqual(spoken, ["free:Blue", "free:Red", "moderation:White", "summary:White"]);
    assert.strictEqual(state.status, "settled");
    assert.include(state.report!.text, "Configured White's summary.");
  }).pipe(Effect.provide(runtime)),
);

it.effect("White's NEXT decides the next speaker, including repeats, then DONE summarizes", () =>
  Effect.gen(function* () {
    yield* boot;
    let state = yield* register(free(["Blue", "Red", "Green"], 9));
    const spoken: string[] = [];
    const prompts: string[] = [];
    for (const text of [
      "Blue opens.",
      "Red opens.",
      "Green opens.",
      "Red, go deeper.\nNEXT: Red",
      "Red goes deeper.",
      "  \nNEXT: Red\n\n",
      "Red again.",
      "We have enough.\nDONE",
      "Closing summary.",
    ]) {
      const step = yield* answer(state, { text });
      spoken.push(step.spoke);
      prompts.push(step.prompt);
      state = step.next;
    }
    assert.deepStrictEqual(spoken, [
      "free:Blue",
      "free:Red",
      "free:Green",
      "moderation:White",
      "free:Red",
      "moderation:White",
      "free:Red",
      "moderation:White",
      "summary:White",
    ]);
    assert.include(prompts[3]!, "turn 3 of 9");
    assert.include(prompts[5]!, "turn 4 of 9");
    assert.include(prompts[7]!, "turn 5 of 9");
    assert.strictEqual(state.status, "settled");
    const transcript = yield* transcriptTexts(state.threadId);
    assert.includeMembers(transcript, [
      "[White]\nRed, go deeper.",
      "[Red]\nRed again.",
      "[White]\nWe have enough.",
      "[White]\nClosing summary.",
    ]);
    assert.isFalse(transcript.some((text) => text.includes("NEXT:") || text.includes("DONE")));
    assert.include(state.report!.text, "Closing summary.");
  }).pipe(Effect.provide(runtime)),
);

it.effect("a Color labelled White speaks as a Color; the moderator is White (moderator)", () =>
  Effect.gen(function* () {
    yield* boot;
    let state = yield* register(free(["White", "Red"], 6));
    const spoken: string[] = [];
    const prompts: string[] = [];
    for (const text of [
      "White opens.",
      "Red opens.",
      "Back to the White Color.\nNEXT: White",
      "The White Color again.",
      "Enough.\nDONE",
      "Summary.",
    ]) {
      const step = yield* answer(state, { text });
      spoken.push(step.spoke);
      prompts.push(step.prompt);
      state = step.next;
    }
    // NEXT: White names the Color, never the moderator.
    assert.deepStrictEqual(spoken, [
      "free:White",
      "free:Red",
      "moderation:White (moderator)",
      "free:White",
      "moderation:White (moderator)",
      "summary:White (moderator)",
    ]);
    assert.include(prompts[2]!, "You are White (moderator), moderating");
    assert.include(prompts[2]!, "(one of: White, Red)");
    const transcript = yield* transcriptTexts(state.threadId);
    assert.includeMembers(transcript, [
      "[White]\nThe White Color again.",
      "[White (moderator)]\nBack to the White Color.",
      "[White (moderator)]\nSummary.",
    ]);
  }).pipe(Effect.provide(runtime)),
);

it.effect("the cap ends the discussion and White always moderates after the opening round", () =>
  Effect.gen(function* () {
    yield* boot;
    let state = yield* register(free(["Blue", "Red"], 3));
    const spoken: string[] = [];
    const prompts: string[] = [];
    for (const text of ["Blue opens.", "Red opens.", "NEXT: Red", "Red's last turn.", "Summary."]) {
      const step = yield* answer(state, { text });
      spoken.push(step.spoke);
      prompts.push(step.prompt);
      state = step.next;
    }
    // The third Color turn reaches the cap, so the summary follows with no moderation call.
    assert.deepStrictEqual(spoken, [
      "free:Blue",
      "free:Red",
      "moderation:White",
      "free:Red",
      "summary:White",
    ]);
    assert.include(prompts[2]!, "turn 2 of 3");
    assert.strictEqual(state.status, "settled");

    let minimal = yield* register({
      ...free(["Blue", "Red"], 2),
      commandId: CommandId.make("register:minimal"),
      threadId: ThreadId.make("spectrum:minimal"),
    });
    const order: string[] = [];
    for (const text of ["Blue opens.", "Red opens.", "NEXT: Red", "Summary."]) {
      const step = yield* answer(minimal, { text });
      order.push(step.spoke);
      minimal = step.next;
    }
    // A NEXT past the cap goes straight to the summary.
    assert.deepStrictEqual(order, ["free:Blue", "free:Red", "moderation:White", "summary:White"]);
    assert.strictEqual(minimal.status, "settled");

    const short = yield* (yield* Launch.SpectrumLaunchService)
      .register({
        ...free(["Blue", "Red"], 1),
        commandId: CommandId.make("register:short"),
        threadId: ThreadId.make("spectrum:short"),
      })
      .pipe(Effect.exit);
    assert.strictEqual(short._tag, "Failure");
  }).pipe(Effect.provide(runtime)),
);

it.effect(
  "a malformed control line is re-asked once, then the discussion ends with the summary",
  () =>
    Effect.gen(function* () {
      yield* boot;
      let state = yield* register(free(["Blue", "Red"], 6));
      const spoken: string[] = [];
      const prompts: string[] = [];
      for (const text of [
        "Blue opens.",
        "Red opens.",
        "Malformed musing.\nNext speaker is Red",
        "Still wrong.\nNEXT: Purple",
        "Summary after a failed re-ask.",
      ]) {
        const step = yield* answer(state, { text });
        spoken.push(step.spoke);
        prompts.push(step.prompt);
        state = step.next;
      }
      assert.deepStrictEqual(spoken, [
        "free:Blue",
        "free:Red",
        "moderation:White",
        "moderation:White",
        "summary:White",
      ]);
      assert.include(prompts[3]!, "did not end with a valid control line");
      assert.strictEqual(state.status, "settled");
      const transcript = yield* transcriptTexts(state.threadId);
      assert.isFalse(
        transcript.some(
          (text) =>
            text.includes("Malformed musing") ||
            text.includes("Still wrong") ||
            text.includes("did not end with a valid control line"),
        ),
      );
      assert.include(transcript, "[White]\nSummary after a failed re-ask.");
    }).pipe(Effect.provide(runtime)),
);

it.effect(
  "a failed summary is retried once and a twice-failed moderation settles failed-summary",
  () =>
    Effect.gen(function* () {
      yield* boot;
      let state = yield* register(free(["Blue", "Red"], 4));
      const spoken: string[] = [];
      for (const outcome of [
        { text: "Blue opens." },
        { text: "Red opens." },
        { text: "DONE" },
        { failed: true as const },
        { text: "Summary on retry." },
      ]) {
        const step = yield* answer(state, outcome);
        spoken.push(step.spoke);
        state = step.next;
      }
      assert.deepStrictEqual(spoken, [
        "free:Blue",
        "free:Red",
        "moderation:White",
        "summary:White",
        "summary:White",
      ]);
      assert.strictEqual(state.status, "settled");
      assert.include(state.report!.text, "Summary on retry.");

      let failing = yield* register({
        ...free(["Blue", "Red"], 4),
        commandId: CommandId.make("register:failing"),
        threadId: ThreadId.make("spectrum:failing"),
      });
      const order: string[] = [];
      for (const outcome of [
        { text: "Blue opens." },
        { text: "Red opens." },
        { failed: true as const },
        { failed: true as const },
      ]) {
        const step = yield* answer(failing, outcome);
        order.push(step.spoke);
        failing = step.next;
      }
      assert.deepStrictEqual(order, [
        "free:Blue",
        "free:Red",
        "moderation:White",
        "moderation:White",
      ]);
      assert.strictEqual(failing.status, "retired");
      assert.match(failing.report!.text, /^Spectrum ended without White's summary/);
      assert.include(failing.report!.text, "Red opens.");
    }).pipe(Effect.provide(runtime)),
);

it.effect("a restart mid-moderation resumes without repeating the call", () =>
  Effect.gen(function* () {
    yield* boot;
    let state = yield* register(free(["Blue", "Red"], 4));
    for (const text of ["Blue opens.", "Red opens."]) state = (yield* answer(state, { text })).next;
    assert.strictEqual(state.round!.phase, "moderation");
    const white = state.white!.threadId;
    const projections = yield* Projection.ProjectionStoreV2;
    const runs = () =>
      projections.getThreadRecords(white, ["runs"]).pipe(Effect.map((r) => r.runs));
    const before = (yield* runs()).length;
    for (let restart = 0; restart < 3; restart++) state = yield* resume(state.threadId);
    assert.strictEqual((yield* runs()).length, before);
    const { run } = yield* currentRun(state);
    yield* end(run, { text: "Malformed." });
    state = yield* resume(state.threadId);
    for (let restart = 0; restart < 3; restart++) state = yield* resume(state.threadId);
    // One re-ask, persisted: restarts never add another.
    assert.strictEqual((yield* runs()).length, before + 1);
    assert.include((yield* currentRun(state)).prompt, "did not end with a valid control line");
  }).pipe(Effect.provide(runtime)),
);

it.effect("Stop during a free discussion settles with an explicit failed-summary outcome", () =>
  Effect.gen(function* () {
    yield* boot;
    let state = yield* register(free(["Blue", "Red"], 4));
    state = (yield* answer(state, { text: "Blue opens." })).next;
    yield* (yield* Orchestrator.OrchestratorV2).dispatch({
      type: "thread.stop",
      threadId: state.threadId,
      commandId: CommandId.make("free:stop"),
    });
    const stopped = Option.getOrThrow(yield* readSpectrum(state.threadId));
    assert.strictEqual(stopped.status, "retired");
    assert.strictEqual(stopped.report!.text, "Spectrum stopped before White's summary.");
  }).pipe(Effect.provide(runtime)),
);

it.effect("a twice-failed White call keeps its failed-summary report across a crash", () =>
  Effect.gen(function* () {
    yield* boot;
    let state = yield* register(free(["Blue", "Red"], 4));
    for (const outcome of [
      { text: "Blue opens." },
      { text: "Red opens." },
      { failed: true as const },
    ])
      state = (yield* answer(state, outcome)).next;
    const { run } = yield* currentRun(state);
    yield* end(run, { failed: true });
    // The process dies after the controller's commits and before any later report step.
    const real = yield* Reports.SpectrumReportService;
    const crashed = yield* resume(state.threadId).pipe(
      Effect.provideService(Reports.SpectrumReportService, {
        ...real,
        settle: () => Effect.die("crash before settle"),
        deliver: () => Effect.die("crash before deliver"),
      }),
      Effect.exit,
    );
    // Whatever the crash interrupted, retirement never persists without its report.
    const persisted = Option.getOrThrow(yield* readSpectrum(state.threadId));
    assert.strictEqual(persisted.status, "retired", String(crashed._tag));
    assert.match(persisted.report!.text, /^Spectrum ended without White's summary/);
    const restarted = yield* resume(state.threadId);
    assert.strictEqual(restarted.status, "retired");
    assert.match(restarted.report!.text, /^Spectrum ended without White's summary/);
    const caller = yield* (yield* Projection.ProjectionStoreV2).getThreadRecords(
      input.callerThreadId,
      ["messages"],
    );
    // A retired report waits for owned effects to drain; it stays owed until it is sent.
    assert.isTrue(
      caller.messages.some((message) => message.id === restarted.report!.messageId) ||
        restarted.outbox.some((command) => command.commandId === restarted.report!.commandId),
    );
  }).pipe(Effect.provide(runtime)),
);

it.effect("a Color's own NEXT or DONE line is discussion text, not a control line", () =>
  Effect.gen(function* () {
    yield* boot;
    let state = yield* register(free(["Blue", "Red", "Green"], 9));
    const spoken: string[] = [];
    for (const text of [
      "Blue opens.",
      "Red opens.\nDONE",
      "Green opens.\nNEXT: Red",
      "NEXT: Green",
      "Green, please.\nNEXT: Red",
    ]) {
      const step = yield* answer(state, { text });
      spoken.push(step.spoke);
      state = step.next;
    }
    assert.deepStrictEqual(spoken, [
      "free:Blue",
      "free:Red",
      "free:Green",
      "moderation:White",
      "free:Green",
    ]);
    // Only White's moderation steers: Green's own NEXT line led back to White, not to Red.
    assert.strictEqual(state.round!.phase, "moderation");
    const transcript = yield* transcriptTexts(state.threadId);
    assert.includeMembers(transcript, [
      "[Red]\nRed opens.\nDONE",
      "[Green]\nGreen opens.\nNEXT: Red",
    ]);
  }).pipe(Effect.provide(runtime)),
);

it.effect("Stop during a moderation call cancels it and settles failed-summary", () =>
  Effect.gen(function* () {
    yield* boot;
    let state = yield* register(free(["Blue", "Red"], 4));
    for (const text of ["Blue opens.", "Red opens."]) state = (yield* answer(state, { text })).next;
    assert.strictEqual(state.round!.phase, "moderation");
    const { run } = yield* currentRun(state);
    yield* (yield* Orchestrator.OrchestratorV2).dispatch({
      type: "thread.stop",
      threadId: state.threadId,
      commandId: CommandId.make("free:stop:moderation"),
    });
    const stopped = Option.getOrThrow(yield* readSpectrum(state.threadId));
    assert.strictEqual(stopped.status, "retired");
    assert.isNull(stopped.round);
    assert.strictEqual(stopped.report!.text, "Spectrum stopped before White's summary.");
    assert.isTrue(
      stopped.outbox.some(
        (command) =>
          (command.type === "run.interrupt" || command.type === "queued-run.cancel") &&
          command.runId === run.id,
      ),
    );
  }).pipe(Effect.provide(runtime)),
);
