import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import {
  readProviderReplayTranscript,
  materializeReplayTranscriptWorkspace,
} from "@t3tools/provider-testing/replayTranscript";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import { CodexOrchestratorReplayHarness as harness } from "../Adapters/CodexAdapterV2.testkit.ts";
import { provideDeterministicTestRuntime } from "./DeterministicRuntime.ts";
import { materializeReplayTranscriptRuntimeInstructions } from "./ReplayRuntimeInstructions.ts";
import { ORCHESTRATOR_REPLAY_FIXTURES } from "./fixtures/index.ts";
import { materializeFixtureInput } from "./fixtures/shared.ts";
import * as Layer from "effect/Layer";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as Stream from "effect/Stream";
import * as EventStore from "../EventStore.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import { runOrchestratorV2Scenario } from "./OrchestratorScenario.ts";
import { layerProviderReplay } from "./ProviderReplayHarness.ts";

it.effect.each(["tool_call_workspace_never", "subagent_v2_nested"])(
  "persists the recorded command states and complete transcript: %s",
  (name) =>
    Effect.gen(function* () {
      const fixture = ORCHESTRATOR_REPLAY_FIXTURES.find((f) => f.name === name)!;
      const driver = fixture.providers.find((p) => p.driver === "codex")!;
      const raw = yield* readProviderReplayTranscript(driver.transcriptFile).pipe(
        Effect.provide(NodeServices.layer),
      );
      const replay = materializeReplayTranscriptRuntimeInstructions(raw, {
        driver: driver.driver,
        model: driver.modelSelection.model,
      });
      const input = fixture.buildInput();
      const workspace = yield* checkpointWorkspace(fixture.name, input.workspaceFiles);
      const transcript = yield* harness.decodeTranscript(
        materializeReplayTranscriptWorkspace(replay, workspace),
      );
      const materialized = yield* materializeFixtureInput({
        scenario: fixture.name,
        fixtureInput: input,
        driver: driver.driver,
        modelSelection: driver.modelSelection,
      }).pipe(Effect.provide(IdAllocator.layer), provideDeterministicTestRuntime);
      const scenario = {
        name: fixture.name,
        transcript,
        commands: materialized.commands,
        steps: materialized.steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { ...driver.runtimePolicyOverride, cwd: workspace },
      };
      return yield* Effect.gen(function* () {
        const result = yield* runOrchestratorV2Scenario(scenario);
        driver.assertOutput(result, transcript);
        const eventStore = yield* EventStore.EventStoreV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const commands = result.domainEvents.filter(
          (event) =>
            event.type === "turn-item.updated" && event.payload.type === "command_execution",
        );
        const replayed = [];
        for (const threadId of new Set(commands.map((event) => event.threadId))) {
          const events = yield* eventStore
            .read({ threadId, eventType: "turn-item.updated" })
            .pipe(Stream.runCollect);
          replayed.push(...events.map((stored) => stored.event));
          const after = yield* projections.getThreadProjection(threadId);
          assert.deepEqual(after.turnItems, result.projections.get(threadId)!.turnItems);
          assert.deepEqual(
            after.visibleTurnItems,
            result.projections.get(threadId)!.visibleTurnItems,
          );
        }
        const counts = new Map<string, { count: number; payloads: Set<string> }>();
        for (const event of replayed) {
          if (event.type !== "turn-item.updated" || event.payload.type !== "command_execution")
            continue;
          const id = event.payload.id;
          const entry = counts.get(id) ?? { count: 0, payloads: new Set<string>() };
          entry.count++;
          entry.payloads.add(JSON.stringify(event.payload));
          counts.set(id, entry);
        }
        for (const entry of counts.values()) {
          assert.equal(
            entry.count,
            2,
            "the recorded command must keep its start and complete states",
          );
          assert.equal(entry.payloads.size, 2);
        }
        assert.isAbove(counts.size, 0);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            layerProviderReplay(scenario, harness, {
              databaseLayer: SqlitePersistence.layerMemory,
            }),
            Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
              Layer.provide(SqlitePersistence.layerMemory),
            ),
          ),
        ),
        provideDeterministicTestRuntime,
      );
    }).pipe(Effect.scoped),
);
