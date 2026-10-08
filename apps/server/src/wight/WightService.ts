import { CommandId, MessageId, ThreadId, type ServerSettingsError } from "@t3tools/contracts";
import { randomUuidV4 } from "../orchestration-v2/RandomUuid.ts";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as ServerSettings from "../serverSettings.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import { isThreadRetired } from "../childThreads/retirement.ts";
import { forkParked } from "../serverActivation.ts";
import { readWightThread } from "./admission.ts";
import { makeWightMode } from "./wightMode.ts";

export class WightMode extends Context.Service<
  WightMode,
  {
    readonly reconcile: Effect.Effect<void>;
  }
>()("t3/wight/WightService/WightMode") {}

const make = Effect.gen(function* () {
  const settings = yield* ServerSettings.ServerSettingsService;
  const registry = yield* ProviderRegistry.ProviderRegistry;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sql = yield* SqlClient.SqlClient;
  const runtime = yield* makeWightMode<
    ServerSettingsError | ProjectionStore.ProjectionStoreV2Error | SqlError,
    never
  >({
    settings: settings.getSettings,
    providers: registry.getProviders,
    retired: (id) =>
      isThreadRetired(ThreadId.make(id)).pipe(
        Effect.provideService(ProjectionStore.ProjectionStoreV2, projections),
      ),
    thread: (id) =>
      readWightThread(ThreadId.make(id)).pipe(
        Effect.provideService(ProjectionStore.ProjectionStoreV2, projections),
        Effect.provideService(SqlClient.SqlClient, sql),
      ),
    resume: Effect.fn("Wight.resume")(function* (thread, text, activation, admission) {
      if (!(yield* admission)) return;
      const id = `server:wight:${yield* randomUuidV4}`;
      yield* threads
        .dispatch({
          type: "message.dispatch",
          threadId: thread.id,
          commandId: CommandId.make(id),
          messageId: MessageId.make(id),
          text,
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "server",
          wightAdmission: {
            latestRunId: thread.latestRunId,
            updatedAt: DateTime.toEpochMillis(thread.updatedAt),
            providerInstanceId: thread.providerInstanceId,
            enabledAt: activation.enabledAt,
          },
        })
        .pipe(
          Effect.catchCause((cause) =>
            Effect.logDebug("Wight admission refused", { threadId: thread.id, cause }),
          ),
        );
    }),
  });
  const reconcile = runtime
    .reconcile()
    .pipe(
      Effect.catchCause((cause) => Effect.logWarning("Wight reconciliation failed", { cause })),
    );
  const changes = yield* settings.subscribeChanges;
  yield* forkParked(
    Stream.mergeAll(
      [
        changes.pipe(Stream.map(() => undefined)),
        registry.streamChanges.pipe(Stream.map(() => undefined)),
        threads.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" ||
              event.type === "run.created" ||
              event.type === "thread.metadata-updated" ||
              event.type === "runtime-request.updated",
          ),
          Stream.map(() => undefined),
        ),
      ],
      { concurrency: "unbounded" },
    ).pipe(Stream.runForEach(() => reconcile)),
  );
  yield* forkParked(reconcile);
  return WightMode.of({ reconcile });
});

export const layer = Layer.effect(WightMode, make);
