import type {
  OrchestrationV2DomainEvent,
  OrchestrationV2RuntimeRequest,
  OrchestrationV2TurnItem,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { ForkCommitGuardRejected, type ForkCommitPlan } from "./ForkCommitPlan.ts";

const encode = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const shape = (
  request: OrchestrationV2RuntimeRequest,
  item: OrchestrationV2TurnItem | undefined,
) => ({
  nodeId: request.nodeId,
  kind: request.kind,
  providerTurnId: request.providerTurnId,
  nativeRequestRef: request.nativeRequestRef,
  responseCapability: request.responseCapability,
  item:
    item?.type === "user_input_request"
      ? { type: item.type, questions: item.questions }
      : item?.type === "approval_request"
        ? {
            type: item.type,
            requestKind: item.requestKind,
            prompt: item.prompt,
            appName: item.appName,
            options: item.options,
          }
        : undefined,
});

/** Intake can commit between command planning and acceptance. This guard runs
 * with events, receipt and outbox in the same SQLite transaction, never on replay.
 */
export function pendingResponseAdmission(
  input: {
    readonly threadId: ThreadId;
    readonly requestId: RuntimeRequestId;
    readonly expectedRevision?: number | undefined;
  },
  events: ReadonlyArray<OrchestrationV2DomainEvent>,
): ForkCommitPlan | undefined {
  const resolution = events.find(
    (event) =>
      event.type === "runtime-request.updated" &&
      event.threadId === input.threadId &&
      event.payload.id === input.requestId &&
      event.payload.status === "resolved",
  );
  if (resolution?.type !== "runtime-request.updated") return undefined;
  const item = events.find(
    (event) =>
      event.type === "turn-item.updated" &&
      event.threadId === input.threadId &&
      (event.payload.type === "user_input_request" || event.payload.type === "approval_request") &&
      event.payload.requestId === input.requestId,
  );
  const plannedShape = shape(
    resolution.payload,
    item?.type === "turn-item.updated" ? item.payload : undefined,
  );
  return {
    guards: [
      Effect.gen(function* () {
        const reject = (kind: "state_conflict" | "storage_failure") =>
          new ForkCommitGuardRejected({ threadId: input.threadId, kind });
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const current = yield* store
          .getRuntimeResponseContext(input.threadId, input.requestId)
          .pipe(Effect.mapError(() => reject("storage_failure")));
        if (current.request?.status !== "pending") return yield* reject("state_conflict");
        if (input.expectedRevision !== undefined) {
          const sql = yield* SqlClient.SqlClient;
          const rows = yield* sql<{
            revision: number;
          }>`SELECT COALESCE(MAX(sequence),0) AS revision FROM orchestration_events WHERE stream_id=${input.threadId} AND ((event_type='runtime-request.updated' AND json_extract(payload_json,'$.id')=${input.requestId}) OR (event_type='turn-item.updated' AND json_extract(payload_json,'$.requestId')=${input.requestId}))`.pipe(
            Effect.mapError(() => reject("storage_failure")),
          );
          if (rows[0]?.revision !== input.expectedRevision) return yield* reject("state_conflict");
        }
        const expected = yield* encode(plannedShape).pipe(
          Effect.mapError(() => reject("storage_failure")),
        );
        const actual = yield* encode(shape(current.request, current.item)).pipe(
          Effect.mapError(() => reject("storage_failure")),
        );
        if (actual !== expected) return yield* reject("state_conflict");
      }),
    ],
    mutations: [],
  };
}
