import { CommandId, SpectrumTranscriptAppend, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { spectrumPlan } from "./spectrumPlan.ts";
import { readSpectrum } from "./store.ts";
import { planTranscriptAppend } from "./transcript.ts";

const decodeAppend = Schema.decodeEffect(SpectrumTranscriptAppend);

export class SpectrumTranscriptError extends Schema.TaggedError<SpectrumTranscriptError>()(
  "SpectrumTranscriptError",
  { threadId: ThreadId, commandId: CommandId, cause: Schema.Defect() },
) {}

/** Server-only: reads the command from durable state, never accepts caller-authored text. */
export class SpectrumTranscriptService extends Context.Service<
  SpectrumTranscriptService,
  {
    readonly dispatch: (
      threadId: ThreadId,
      commandId: CommandId,
    ) => Effect.Effect<CommandReceiptStore.CommandReceiptV2, SpectrumTranscriptError>;
  }
>()("t3/spectrum/TranscriptService/SpectrumTranscriptService") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sink = yield* EventSink.EventSinkV2;
  const dispatch = Effect.fn("SpectrumTranscript.dispatch")(function* (
    threadId: ThreadId,
    commandId: CommandId,
  ) {
    const receipt = yield* receipts.getByCommandId(commandId);
    if (Option.isSome(receipt)) {
      if (
        receipt.value.threadId !== threadId ||
        receipt.value.commandType !== "spectrum.transcript.append" ||
        receipt.value.status !== "accepted"
      )
        return yield* Effect.fail(
          new SpectrumTranscriptError({ threadId, commandId, cause: "Receipt mismatch" }),
        );
      return receipt.value;
    }
    const found = yield* readSpectrum(threadId).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
    );
    if (Option.isNone(found))
      return yield* Effect.fail(
        new SpectrumTranscriptError({ threadId, commandId, cause: "Spectrum is not registered" }),
      );
    const state = found.value;
    const pending = state.outbox.find((command) => command.commandId === commandId);
    if (pending?.type !== "spectrum.transcript.append")
      return yield* Effect.fail(
        new SpectrumTranscriptError({ threadId, commandId, cause: "Missing durable command" }),
      );
    const command = yield* decodeAppend(pending);
    const thread = yield* projections.getThread(threadId);
    const now = yield* DateTime.now;
    const planned = yield* Effect.try(() => planTranscriptAppend(state, thread, command, now));
    const result = yield* sink.commitCommand({
      commandId,
      threadId,
      commandType: command.type,
      acceptedAt: now,
      events: planned.events,
      effects: [],
      forkPlans: [spectrumPlan(planned.mutation)],
    });
    if (
      result.receipt.threadId !== threadId ||
      result.receipt.commandType !== command.type ||
      result.receipt.status !== "accepted"
    )
      return yield* Effect.fail(
        new SpectrumTranscriptError({ threadId, commandId, cause: "Receipt mismatch" }),
      );
    return result.receipt;
  });
  return SpectrumTranscriptService.of({
    dispatch: (threadId, commandId) =>
      dispatch(threadId, commandId).pipe(
        Effect.mapError((cause) => new SpectrumTranscriptError({ threadId, commandId, cause })),
      ),
  });
});

export const layer = Layer.effect(SpectrumTranscriptService, make);
