import { assert, describe, it } from "@effect/vitest";
import { CommandId, EventId, RunId } from "@t3tools/contracts";
import * as SqlClient from "effect/sql/SqlClient";
import * as Effect from "effect/Effect";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Store from "./RecoveryStore.ts";
import {
  recoveryTestLayer,
  recoveryRun as run,
  recoveryEvents,
  errorFor,
} from "./recovery.testkit.ts";
import { retryAdmission } from "./recoveryAdmission.ts";
import { retryCommand } from "./recoveryPolicy.ts";

const setup = Effect.gen(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const store = yield* Store.RecoveryStore;
  yield* sink.commitCommand({
    commandId: CommandId.make("seed"),
    threadId: run.threadId,
    commandType: "seed",
    acceptedAt: run.requestedAt,
    events: recoveryEvents(run),
    effects: [],
  });
  const failure = errorFor(run);
  if (failure.type !== "error") throw new Error("fixture");
  const armed = yield* store.reconcile({
    previous: null,
    run,
    failure: failure.failure,
    stoppedOrRetired: false,
    autoResume: true,
  });
  const command = retryCommand(armed);
  if (command?.type !== "message.dispatch") throw new Error("fixture");
  const plan = retryAdmission(command)!;
  return { sink, store, armed, command, plan };
});
describe("Prism atomic retry admission", () => {
  it.effect(
    "duplicate dispatch commits one retry and receipt replay bypasses consumed eligibility",
    () =>
      Effect.gen(function* () {
        const { sink, store, command, plan } = yield* setup;
        const retryRun = {
          ...run,
          id: RunId.make("run:committed-retry"),
          ordinal: 2,
          userMessageId: command.messageId,
          status: "running" as const,
          completedAt: null,
        };
        const input = {
          commandId: command.commandId,
          threadId: command.threadId,
          commandType: command.type,
          acceptedAt: run.requestedAt,
          events: [
            {
              id: EventId.make("event:retry"),
              type: "run.created" as const,
              threadId: command.threadId,
              occurredAt: run.requestedAt,
              payload: retryRun,
            },
          ],
          effects: [
            {
              id: "effect:retry",
              commandId: command.commandId,
              threadId: command.threadId,
              request: { type: "provider-turn.start" as const, runId: retryRun.id },
            },
          ],
          forkPlans: [plan],
        };
        const receipts = yield* Effect.all([sink.commitCommand(input), sink.commitCommand(input)], {
          concurrency: "unbounded",
        });
        assert.deepStrictEqual(receipts[0]?.receipt, receipts[1]?.receipt);
        assert.strictEqual((yield* store.get(run.threadId))?.state, "retry_started");
        const replay = yield* sink.commitCommand(input);
        assert.deepStrictEqual(replay.receipt, receipts[0]?.receipt);
        const sql = yield* SqlClient.SqlClient;
        const effects = yield* sql<{
          count: number;
        }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox WHERE command_id=${command.commandId}`;
        assert.strictEqual(effects[0]?.count, 1);
      }).pipe(Effect.provide(recoveryTestLayer)),
  );
  it.effect(
    "closing recovery after plan construction rejects dispatch and leaves finalization released",
    () =>
      Effect.gen(function* () {
        const { sink, store, armed, command, plan } = yield* setup;
        yield* store.close(armed);
        const rejected = yield* sink
          .commitCommand({
            commandId: command.commandId,
            threadId: command.threadId,
            commandType: command.type,
            acceptedAt: run.requestedAt,
            events: [],
            effects: [],
            forkPlans: [plan],
          })
          .pipe(Effect.flip);
        assert.strictEqual(rejected._tag, "ForkCommitGuardRejected");
        assert.strictEqual((yield* store.get(run.threadId))?.state, "closed");
      }).pipe(Effect.provide(recoveryTestLayer)),
  );
  it.effect("a changed provider/model is refused without consuming the retry", () =>
    Effect.gen(function* () {
      const { sink, store, command } = yield* setup;
      const altered = { ...command, modelSelection: { ...run.modelSelection, model: "different" } };
      const rejected = yield* sink
        .commitCommand({
          commandId: command.commandId,
          threadId: command.threadId,
          commandType: command.type,
          acceptedAt: run.requestedAt,
          events: [],
          effects: [],
          forkPlans: [retryAdmission(altered)!],
        })
        .pipe(Effect.flip);
      assert.strictEqual(rejected._tag, "ForkCommitGuardRejected");
      assert.strictEqual((yield* store.get(run.threadId))?.state, "retry_pending");
    }).pipe(Effect.provide(recoveryTestLayer)),
  );
});
