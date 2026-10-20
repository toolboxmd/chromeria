import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  EnvironmentId,
  ProviderInstanceId,
  RunId,
  type SpectrumSettings,
  DEFAULT_SPECTRUM_SETTINGS,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import * as Access from "../mcp/McpToolAccess.ts";
import { idleThreadProjection, liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Events from "../orchestration-v2/EventSink.ts";
import { makeRun, NOW } from "./testFixtures.ts";
import { ensureCheckSchema } from "../scheduledTaskChecks/store.ts";
import * as Settings from "../serverSettings.ts";
import { base, input, setup, spareInstanceId } from "./controllerTestkit.ts";
import * as Launch from "./LaunchService.ts";
import { SpectrumToolkit, StartSpectrumInput, StartSpectrumResult, layer } from "./mcpToolkit.ts";
import { readSpectrum } from "./store.ts";

const decodeStart = Schema.decodeUnknownEffect(StartSpectrumResult);

const primary = { instanceId: ProviderInstanceId.make("codex"), model: "test-model" };
const defaults: SpectrumSettings = {
  slots: [
    { label: "Blue", primary, fallbacks: [{ ...primary, instanceId: spareInstanceId }] },
    { label: "Red", primary, fallbacks: [] },
  ],
  white: { primary: { ...primary, instanceId: spareInstanceId }, fallbacks: [] },
  mode: "council",
  relayRounds: 4,
  turnsPerColor: 5,
};
const caller = liveThreadShell(input.callerThreadId, { activeRunId: RunId.make("caller-run") });
const dependencies = Layer.mergeAll(
  base,
  NodeServices.layer,
  Settings.layerTest({ spectrum: defaults }),
  Layer.succeed(Invocation.McpInvocationContext, {
    environmentId: EnvironmentId.make("test"),
    requestNamespace: "settings-test",
    thread: {
      threadId: caller.id,
      providerSessionId: "test",
      providerInstanceId: primary.instanceId,
    },
    client: undefined,
    capabilities: new Set(["orchestration"] as const),
    issuedAt: 1,
  }),
  Layer.mock(Threads.ThreadManagementService)({
    getThreadShell: () => Effect.succeed(caller),
    getProjectThreadRecords: () => Effect.succeed(idleThreadProjection(caller)),
  }),
);
const runtime = Layer.fresh(
  Layer.mergeAll(dependencies, Launch.layer.pipe(Layer.provide(dependencies))),
);

it.effect(
  "start_spectrum uses defaults, explicit overrides and persists settings once across replay",
  () =>
    Effect.gen(function* () {
      const callerRecord = yield* setup;
      yield* ensureCheckSchema;
      yield* (yield* Events.EventSinkV2).write({
        events: [
          {
            id: EventId.make("caller-effort"),
            type: "thread.model-selection-updated",
            threadId: callerRecord.id,
            occurredAt: NOW,
            payload: {
              ...callerRecord,
              modelSelection: {
                ...callerRecord.modelSelection,
                options: [{ id: "reasoningEffort", value: "high" }],
              },
            },
          },
        ],
      });

      const run = makeRun(
        {
          threadId: caller.id,
          commandId: CommandId.make("caller-command"),
          messageId: MessageId.make("caller-message"),
          runId: null,
          replyIds: null,
        },
        { id: RunId.make("caller-run"), status: "running", completedAt: null },
      );
      yield* (yield* Events.EventSinkV2).write({
        events: [
          {
            id: EventId.make("caller-run-created"),
            type: "run.updated",
            threadId: caller.id,
            occurredAt: NOW,
            payload: run,
          },
        ],
      });
      const toolkit = yield* SpectrumToolkit.pipe(
        Effect.provide(Access.HandlersLayer.layer(layer)),
      );
      const call = (value: typeof StartSpectrumInput.Type) =>
        toolkit.handle("start_spectrum", value).pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.flatMap((results) => {
            const success = results.find((result) => !result.isFailure);
            return success === undefined
              ? Effect.die("Expected a successful native start_spectrum result")
              : decodeStart(success.result);
          }),
        );
      const started = yield* call({ question: "Question only", clientRequestId: "default" });
      const id = started.threadId;
      const first = Option.getOrThrow(yield* readSpectrum(id));
      expect(first).toMatchObject({
        question: "Question only",
        mode: "council",
        limit: 4,
        turnsPerColor: 5,
      });
      // The saved White is its own member with its own thread, beside the two saved slots.
      expect(first.moderator).toBeUndefined();
      expect(first.participants.map((color) => color.label)).toEqual(["Blue", "Red"]);
      expect(first.white).toMatchObject({
        threadId: `sub.${id}.white`,
        label: "White",
        selection: { ...primary, instanceId: spareInstanceId },
      });
      expect(first.participants[0]?.selection.options).toBeUndefined();
      expect(first.participants[0]?.fallbacks[0]?.options).toBeUndefined();
      expect(first.participants[0]?.fallbacks).toEqual([
        { ...primary, instanceId: spareInstanceId },
      ]);
      yield* (yield* Settings.ServerSettingsService).updateSettings({
        spectrum: { ...defaults, relayRounds: 7 },
      });
      const replayed = yield* call({ question: "Replay", clientRequestId: "default" });
      expect(replayed.threadId).toBe(id);
      expect(replayed).toEqual(started);
      expect(Option.getOrThrow(yield* readSpectrum(id))).toEqual(first);
      yield* (yield* Settings.ServerSettingsService).updateSettings({
        projectSettingsOverrides: {
          [callerRecord.projectId]: { spectrum: { ...defaults, mode: "free", turnsPerColor: 2 } },
        },
      });
      const project = yield* call({ question: "Project defaults", clientRequestId: "project" });
      expect(Option.getOrThrow(yield* readSpectrum(project.threadId))).toMatchObject({
        mode: "free",
        limit: 4,
        turnsPerColor: 2,
        white: { selection: { ...primary, instanceId: spareInstanceId } },
      });
      for (const [key, preferences, expected] of [
        ["large-council", { ...defaults, relayRounds: 101 }, 101],
        ["large-free", { ...defaults, mode: "free" as const, turnsPerColor: 12 }, 24],
      ] as const) {
        yield* (yield* Settings.ServerSettingsService).updateSettings({
          projectSettingsOverrides: { [callerRecord.projectId]: { spectrum: preferences } },
        });
        const configured = yield* call({ question: "Configured limit", clientRequestId: key });
        expect(Option.getOrThrow(yield* readSpectrum(configured.threadId)).limit).toBe(expected);
      }
      const explicitResult = yield* call({
        question: "Explicit",
        clientRequestId: "explicit",
        colors: [
          { label: "Blue", ...primary },
          { label: "Red", ...primary, fallbacks: [{ ...primary, instanceId: spareInstanceId }] },
        ],
        mode: "free",
        limit: 7,
        moderator: 0,
        turnsPerColor: 2,
      });
      const explicit = Option.getOrThrow(yield* readSpectrum(explicitResult.threadId));
      expect(explicit).toMatchObject({ mode: "free", limit: 7, turnsPerColor: 2 });
      // A legacy moderator index copies that Color's model for White; the Color still speaks.
      expect(explicit.participants).toHaveLength(2);
      expect(explicit.white?.selection).toEqual(explicit.participants[0]?.selection);
      expect(explicit.participants[0]?.fallbacks).toEqual([]);
      expect(explicit.participants[1]?.fallbacks).toEqual([
        {
          ...primary,
          instanceId: spareInstanceId,
          options: [{ id: "reasoningEffort", value: "high" }],
        },
      ]);
      for (const [key, labels] of [
        ["explicit-white-first", ["Sol", "Grok"]],
        ["explicit-white-matching-label", ["Sol", "Red"]],
      ] as const) {
        const result = yield* call({
          question: "Explicit slots choose White",
          clientRequestId: key,
          colors: labels.map((label) => ({ label, ...primary })),
        });
        const stored = Option.getOrThrow(yield* readSpectrum(result.threadId));
        // Explicit Colors ignore the saved White: White copies the first Color's model.
        expect(stored.white?.selection).toEqual(stored.participants[0]?.selection);
        expect(stored.white?.selection.instanceId).not.toBe(spareInstanceId);
        expect(stored.participants.map((color) => color.label)).toEqual(labels);
        expect(stored.participants.every((color) => color.fallbacks.length === 0)).toBe(true);
      }
      const judged = yield* call({
        question: "Explicit White",
        clientRequestId: "explicit-white-color",
        colors: [
          { label: "Blue", ...primary },
          { label: "Red", ...primary },
        ],
        white: { ...primary, instanceId: spareInstanceId, fallbacks: [{ ...primary }] },
      });
      expect(judged.white).toMatchObject({ label: "White", instanceId: spareInstanceId });
      const withWhite = Option.getOrThrow(yield* readSpectrum(judged.threadId));
      expect(withWhite.white).toMatchObject({
        selection: { instanceId: spareInstanceId },
        fallbacks: [{ instanceId: primary.instanceId }],
      });
      expect(withWhite.participants.map((color) => color.label)).toEqual(["Blue", "Red"]);
    }).pipe(Effect.provide(runtime)),
);

it.effect("start_spectrum rejects invalid values before persisting a Spectrum", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* ensureCheckSchema;
    const run = makeRun(
      {
        threadId: caller.id,
        commandId: CommandId.make("caller-command"),
        messageId: MessageId.make("caller-message"),
        runId: null,
        replyIds: null,
      },
      { id: RunId.make("caller-run"), status: "running", completedAt: null },
    );
    yield* (yield* Events.EventSinkV2).write({
      events: [
        {
          id: EventId.make("caller-run-created"),
          type: "run.updated",
          threadId: caller.id,
          occurredAt: NOW,
          payload: run,
        },
      ],
    });
    const toolkit = yield* SpectrumToolkit.pipe(Effect.provide(Access.HandlersLayer.layer(layer)));
    const sql = yield* SqlClient.SqlClient;
    const persistedBefore =
      yield* sql`SELECT thread_id,payload_json FROM fork_spectra ORDER BY thread_id`;
    for (const [patch, message] of [
      [{ limit: 1 }, /at least two relay rounds/],
      [{ mode: "free", limit: 1 }, /cap cannot be below the number of Colors/],
      [{ white: "Red", moderator: 0 }, /different Colors/],
      [{ white: { ...primary }, moderator: 0 }, /not both/],
      [{ relayRounds: 1 }, /relayRounds/],
      [{ turnsPerColor: 0 }, /turnsPerColor/],
      [{ white: "Missing" }, /White/],
      [
        {
          colors: [
            { label: "Blue", ...primary },
            { label: "Blue", ...primary },
          ],
        },
        /labels must be unique/,
      ],
    ] as const) {
      const result = yield* toolkit
        .handle("start_spectrum", { question: "Invalid", clientRequestId: "invalid", ...patch })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.exit);
      // Parameter validation and service validation retain their actionable errors.
      if (result._tag === "Success")
        expect(result.value).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              isFailure: true,
              encodedResult:
                "relayRounds" in patch || "turnsPerColor" in patch
                  ? expect.objectContaining({
                      reason: expect.objectContaining({
                        _tag: "ToolParameterValidationError",
                        description: expect.stringMatching(message),
                      }),
                    })
                  : expect.objectContaining({
                      code: "invalid_request",
                      message: expect.stringMatching(message),
                    }),
            }),
          ]),
        );
      expect(
        yield* sql`SELECT thread_id,payload_json FROM fork_spectra ORDER BY thread_id`,
      ).toEqual(persistedBefore);
    }
    yield* (yield* Settings.ServerSettingsService).updateSettings({
      spectrum: DEFAULT_SPECTRUM_SETTINGS,
    });
    const unconfigured = yield* toolkit
      .handle("start_spectrum", { question: "No slots configured" })
      .pipe(Stream.unwrap, Stream.runCollect);
    expect(unconfigured).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          isFailure: true,
          encodedResult: expect.objectContaining({
            code: "invalid_request",
            message: expect.stringContaining("Settings > Spectrum"),
          }),
        }),
      ]),
    );
    expect(yield* sql`SELECT thread_id,payload_json FROM fork_spectra ORDER BY thread_id`).toEqual(
      persistedBefore,
    );
  }).pipe(Effect.provide(runtime)),
);
