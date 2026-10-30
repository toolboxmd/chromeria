import { retainNewerFields } from "../persistence/rollbackCompatibility.ts";
import * as SchemaAST from "effect/SchemaAST";
import * as RemoteSecretRequests from "../secrets/RemoteSecretRequests.ts";
import * as Automation from "./DestinationAutomationAuthority.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import { PeerAcceptedAutomation } from "../peer/peerEffectAuthority.ts";
import { requestDigest } from "../mcp/McpRequestReceipts.ts";
import { SavedPurposeIntent, type ExecutionPurpose } from "@t3tools/contracts";
import * as PurposeAdmission from "../orchestration-v2/ExecutionPurpose.ts";
import * as MutationReceipts from "./mutationReceipts.ts";
import * as CommandReceipts from "../orchestration-v2/CommandReceiptStore.ts";
import { type RequestIdentity } from "../mcp/McpRequestReceipts.ts";
import { BareReadGrant } from "../orchestration-v2/bareReadGrant.ts";
import * as BareLaunch from "../orchestration-v2/BareLaunch.ts";
import { decodeScheduledBare, encodeScheduledBare, readScheduledBare } from "./bareState.ts";
import {
  CommandId,
  MessageId,
  McpManualRunReceipt,
  McpScheduledMutationReceipt,
  PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
  OrchestrationV2LinkOrigin,
  ScheduledTask,
  ScheduledTaskError,
  ScheduledTaskId,
  ThreadId,
  ScheduledTaskWebhookDeliveryId,
  type ScheduledTaskDeleteInput,
  type ScheduledTaskDeleteResult,
  type ScheduledTaskGetWebhookDeliveryInput,
  type ScheduledTaskGetWebhookDeliveryResult,
  type ScheduledTaskListWebhookDeliveriesInput,
  type ScheduledTaskListWebhookDeliveriesResult,
  type ScheduledTaskRotateWebhookTokenInput,
  type ScheduledTaskWebhookDeliveryOutcome,
  type ScheduledTaskListResult,
  type ScheduledTaskMutationResult,
  OrchestratorMcpFailure,
  type ScheduledTaskRunNowInput,
  type ScheduledTaskRunNowResult,
  type ScheduledTaskSetEnabledInput,
  type ScheduledTaskUpsertInput,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Data from "effect/Data";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Duration from "effect/Duration";
import * as Metric from "effect/Metric";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import { dispatchVia, ScheduledTaskDispatchPolicy } from "../scheduledTaskChecks/DispatchPolicy.ts";
import { forkFireKey } from "../scheduledTaskChecks/schedules.ts";
import * as Metrics from "../observability/Metrics.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import { isMissedFixedTimeRun, isSameSchedule, nextScheduledRunAt } from "./Schedule.ts";
import {
  redactHeaders,
  redactQuery,
  renderWebhookPrompt,
  type WebhookRequest,
} from "./webhookTemplate.ts";
import { constantTimeEquals, verifyWebhookSignature } from "./webhookVerification.ts";

export const WEBHOOK_ROUTE_PREFIX = "/api/hooks";
const WEBHOOK_DELIVERY_LOG_BODY_LIMIT = 64 * 1024;
const WEBHOOK_DELIVERY_LOG_PROMPT_LIMIT = 64 * 1024;
const WEBHOOK_MAX_QUEUED_PER_TASK = 20;
/** Accepted deliveries per task per minute, enforced here as well as on the relay because the tunnel hostname is public too. */
const WEBHOOK_RATE_LIMIT_PER_MINUTE = 60;

interface WebhookOrigin {
  readonly relayHookBaseUrl: string | null;
}

const isScheduledTaskError = Schema.is(ScheduledTaskError);
const ENDPOINT_KEY = /^[0-9a-f]{16}$/;

export function relayHookBaseUrl(input: {
  readonly relayUrl: string;
  readonly tunnelName: string | undefined;
}): string | null {
  const endpointKey = input.tunnelName?.split("-").at(-1);
  const relayUrl = input.relayUrl.replace(/\/+$/, "");
  if (relayUrl === "" || endpointKey === undefined || !ENDPOINT_KEY.test(endpointKey)) {
    return null;
  }
  return `${relayUrl}/v1/hooks/${endpointKey}`;
}

export class ScheduledTaskWebhookOrigin extends Context.Reference<Effect.Effect<WebhookOrigin>>(
  "t3/scheduledTasks/ScheduledTaskWebhookOrigin",
  {
    defaultValue: () => Effect.succeed({ relayHookBaseUrl: null }),
  },
) {}

class WebhookDeliverySkipped extends Data.TaggedError("WebhookDeliverySkipped")<{
  readonly reason: string;
}> {}

interface RateWindow {
  readonly accepted: ReadonlyArray<number>;
  readonly rejectedLogged: boolean;
}

export interface WebhookTriggerRequest extends WebhookRequest {
  readonly hookId: string;
  readonly token: string;
  readonly body: Uint8Array;
  readonly relayDeliveryId?: string;
  readonly receivedAt?: string;
}

export type WebhookDeliveryOutcome =
  | "accepted"
  | "duplicate"
  | "prompt_too_long"
  | "queue_full"
  | "rate_limited"
  | "disabled"
  | "rejected_signature"
  | "expired"
  | "not_found"
  | "error";

export type WebhookTriggerResult =
  | {
      readonly _tag: "accepted";
      readonly deliveryId: ScheduledTaskWebhookDeliveryId;
      readonly outcome: "accepted" | "duplicate" | "prompt_too_long";
    }
  | { readonly _tag: "not_found" }
  | { readonly _tag: "rejected_signature" }
  | { readonly _tag: "disabled" }
  | {
      readonly _tag: "rate_limited";
      readonly outcome: "rate_limited" | "queue_full";
    }
  | { readonly _tag: "expired" };

const decodeLinkOriginJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2LinkOrigin),
);
const decodeTask = Schema.decodeUnknownEffect(ScheduledTask);
const decodeTaskId = Schema.decodeUnknownOption(ScheduledTaskId);
const decodePurposeIntentJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(SavedPurposeIntent),
);
const encodeAutomationStamp = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const encodePurposeIntentJson = Schema.encodeSync(Schema.fromJsonString(SavedPurposeIntent));
const decodeScheduleJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Union([
      ScheduledTask.fields.schedule,
      Schema.Struct({
        type: Schema.Literal("destination-automation-v1"),
        certificateId: Schema.String,
        schedule: ScheduledTask.fields.schedule,
      }),
    ]),
  ),
);
const decodeWorkspaceStrategyJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(ScheduledTask.fields.workspaceStrategy),
);
const decodeModelSelectionJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(ScheduledTask.fields.modelSelection),
);
const HeadersJson = Schema.fromJsonString(Schema.Record(Schema.String, Schema.String));
const MissingFieldsJson = Schema.fromJsonString(Schema.Array(Schema.String));
const decodeHeadersJson = Schema.decodeUnknownOption(HeadersJson);
const decodeMissingFieldsJson = Schema.decodeUnknownOption(MissingFieldsJson);
const encodeHeadersJson = Schema.encodeSync(HeadersJson);
const encodeMissingFieldsJson = Schema.encodeSync(MissingFieldsJson);

interface ScheduledTaskRow {
  readonly task_id: string;
  readonly title: string;
  readonly prompt: string;
  readonly enabled: number;
  readonly schedule_json: string;
  readonly purpose_intent_json: string | null;
  readonly project_id: string;
  readonly thread_id: string | null;
  readonly workspace_strategy_json: string;
  readonly model_selection_json: string;
  readonly runtime_mode: string;
  readonly interaction_mode: string;
  readonly created_by: string;
  readonly creation_source: string;
  readonly bare_json: string | null;
  readonly link_origin_json: string | null;
  readonly created_at: string;
  readonly updated_at: string;
  readonly next_run_at: string | null;
  readonly last_run_at: string | null;
  readonly last_run_status: string;
  readonly last_run_error: string | null;
  readonly run_count: number;
  readonly webhook_token: string | null;
  readonly webhook_secret: string | null;
}

interface WebhookDeliveryRow {
  readonly delivery_id: string;
  readonly task_id: string;
  readonly received_at: string;
  readonly method: string;
  readonly query: string;
  readonly headers_json: string;
  readonly body: string;
  readonly body_bytes: number;
  readonly body_truncated: number;
  readonly outcome: string;
  readonly signature_verified: number;
  readonly missing_fields_json: string;
  readonly rendered_prompt: string | null;
  readonly error: string | null;
}

export type ManualOptions = {
  readonly request: RequestIdentity;
  readonly recheck?: Effect.Effect<void, OrchestratorMcpFailure>;
};

export class ScheduledTaskService extends Context.Service<
  ScheduledTaskService,
  {
    readonly mutationReceipt?: (
      request: RequestIdentity,
    ) => Effect.Effect<Option.Option<McpScheduledMutationReceipt>, OrchestratorMcpFailure>;
    readonly list: () => Effect.Effect<ScheduledTaskListResult, ScheduledTaskError>;
    readonly subscribeList: () => Stream.Stream<ScheduledTaskListResult, ScheduledTaskError>;
    readonly upsert: (
      input: ScheduledTaskUpsertInput,
      options?: { readonly bareReadGrant: BareReadGrant },
    ) => Effect.Effect<ScheduledTaskMutationResult, ScheduledTaskError>;
    readonly setEnabled: (
      input: ScheduledTaskSetEnabledInput,
    ) => Effect.Effect<ScheduledTaskMutationResult, ScheduledTaskError>;
    readonly delete: (
      input: ScheduledTaskDeleteInput,
    ) => Effect.Effect<ScheduledTaskDeleteResult, ScheduledTaskError>;
    readonly runNow: {
      (
        input: ScheduledTaskRunNowInput,
      ): Effect.Effect<ScheduledTaskRunNowResult, ScheduledTaskError>;
      (
        input: ScheduledTaskRunNowInput,
        options: ManualOptions,
      ): Effect.Effect<ScheduledTaskRunNowResult, ScheduledTaskError | OrchestratorMcpFailure>;
    };
    /** Issues a new URL token for a webhook task; the old URL stops working at once. */
    readonly rotateWebhookToken: (
      input: ScheduledTaskRotateWebhookTokenInput,
    ) => Effect.Effect<ScheduledTaskMutationResult, ScheduledTaskError>;
    readonly listWebhookDeliveries: (
      input: ScheduledTaskListWebhookDeliveriesInput,
    ) => Effect.Effect<ScheduledTaskListWebhookDeliveriesResult, ScheduledTaskError>;
    readonly getWebhookDelivery: (
      input: ScheduledTaskGetWebhookDeliveryInput,
    ) => Effect.Effect<ScheduledTaskGetWebhookDeliveryResult, ScheduledTaskError>;
    /**
     * Verifies, logs and dispatches one webhook request. Returns as soon as the
     * delivery is logged; the run itself continues in the background so senders
     * with short timeouts get their answer immediately.
     */
    readonly triggerWebhook: (
      request: WebhookTriggerRequest,
    ) => Effect.Effect<WebhookTriggerResult, ScheduledTaskError>;
  }
>()("t3/scheduledTasks/ScheduledTaskService") {}

function taskError(message: string, input?: { taskId?: ScheduledTaskId; cause?: unknown }) {
  return new ScheduledTaskError({
    message,
    ...(input?.taskId === undefined ? {} : { taskId: input.taskId }),
    ...(input?.cause === undefined ? {} : { cause: input.cause }),
  });
}

function iso(value: DateTime.DateTime): string {
  return DateTime.formatIso(DateTime.toUtc(value));
}

const localNow = DateTime.withCurrentZoneLocal(DateTime.nowInCurrentZone);

function nextRunAt(
  task: Pick<ScheduledTask, "enabled" | "schedule">,
  from: DateTime.DateTime,
): string | null {
  if (!task.enabled) return null;
  // A stored interval can decode yet overflow the representable DateTime
  // range; an unrepresentable occurrence means the task has no next run.
  try {
    const next = nextScheduledRunAt(task.schedule, from);
    return next !== null && Number.isFinite(DateTime.toEpochMillis(next)) ? iso(next) : null;
  } catch {
    return null;
  }
}

function errorMessage(error: unknown): string {
  if (Cause.isCause(error)) return Cause.pretty(error);
  if (error instanceof Error) return error.message;
  return String(error);
}

function webhookPath(taskId: string, token: string): string {
  return `${WEBHOOK_ROUTE_PREFIX}/${encodeURIComponent(taskId)}/${token}`;
}

function webhookEndpoint(
  row: Pick<ScheduledTaskRow, "task_id" | "webhook_token" | "webhook_secret">,
  origin: WebhookOrigin | null,
): ScheduledTask["webhook"] {
  if (row.webhook_token === null) return undefined;
  const base = origin?.relayHookBaseUrl ?? null;
  return {
    path: webhookPath(row.task_id, row.webhook_token),
    url: base === null ? null : `${base}/${encodeURIComponent(row.task_id)}/${row.webhook_token}`,
    hasSecret: row.webhook_secret !== null,
  };
}

const decodeRow = (row: ScheduledTaskRow, origin: WebhookOrigin | null = null) =>
  Effect.gen(function* () {
    const stored = yield* decodeScheduleJson(row.schedule_json);
    const schedule = stored.type === "destination-automation-v1" ? stored.schedule : stored;
    const webhook = schedule.type === "webhook" ? webhookEndpoint(row, origin) : undefined;
    const workspaceStrategy = yield* decodeWorkspaceStrategyJson(row.workspace_strategy_json);
    const modelSelection = yield* decodeModelSelectionJson(row.model_selection_json);
    return yield* decodeTask({
      ...(row.bare_json == null
        ? {}
        : {
            bare: (yield* decodeScheduledBare(row.bare_json)).request,
          }),
      // The stored id decodes through the task schema so a corrupt value fails
      // as a typed parse error, not a `ScheduledTaskId.make` defect.
      id: row.task_id,
      title: row.title,
      prompt: row.prompt,
      enabled: row.enabled === 1,
      schedule,
      ...(row.purpose_intent_json == null
        ? {}
        : { purposeIntent: yield* decodePurposeIntentJson(row.purpose_intent_json) }),
      projectId: row.project_id,
      threadId: row.thread_id,
      workspaceStrategy,
      modelSelection,
      runtimeMode: row.runtime_mode,
      interactionMode: row.interaction_mode,
      createdBy: row.created_by,
      creationSource: row.creation_source,
      ...(row.link_origin_json == null
        ? {}
        : { linkOrigin: yield* decodeLinkOriginJson(row.link_origin_json) }),
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      nextRunAt: row.next_run_at,
      lastRunAt: row.last_run_at,
      lastRunStatus: row.last_run_status,
      lastRunError: row.last_run_error,
      runCount: row.run_count,
      ...(webhook === undefined ? {} : { webhook }),
    });
  }).pipe(
    Effect.mapError((cause) => {
      const taskId = decodeTaskId(row.task_id);
      return taskError("Could not decode schedule task row.", {
        ...(Option.isSome(taskId) ? { taskId: taskId.value } : {}),
        cause,
      });
    }),
  );

export const listDueTasks = Effect.fn("ScheduledTaskService.listDueTasks")(function* (
  now: DateTime.DateTime,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<ScheduledTaskRow>`
    SELECT * FROM scheduled_tasks
    WHERE enabled = 1 AND next_run_at IS NOT NULL
      AND next_run_at <= ${iso(now)} AND last_run_status <> 'running'
    ORDER BY next_run_at ASC, task_id ASC
  `;
  const tasks: ScheduledTask[] = [];
  for (const row of rows) {
    const decoded = yield* Effect.result(decodeRow(row));
    if (Result.isSuccess(decoded)) {
      const task = decoded.success;
      // next_run_at is a freeform string at the schema level; a stored value
      // that cannot parse as a DateTime would defect the poll below, so the
      // row is skipped here like any other corrupt row.
      if (task.nextRunAt === null || Option.isSome(DateTime.make(task.nextRunAt))) {
        tasks.push(task);
      } else {
        yield* Effect.logWarning("Skipping schedule task row with invalid next_run_at", {
          taskId: row.task_id,
        });
      }
    } else {
      yield* Effect.logWarning("Skipping undecodable schedule task row", {
        taskId: row.task_id,
        cause: decoded.failure,
      });
    }
  }
  return tasks;
});

export const layer = Layer.effect(
  ScheduledTaskService,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const automation = yield* Automation.DestinationAutomationAuthority;
    const peerAuthority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
    const notifyAutomationChanges = Option.isSome(peerAuthority)
      ? peerAuthority.value.notifyAutomationChanges
      : Effect.void;
    const crypto = yield* Crypto.Crypto;
    const hasDeliveryCorrelation = (yield* sql<{
      name: string;
    }>`PRAGMA table_info(scheduled_tasks)`).some(
      (column) => column.name === "last_run_webhook_delivery_id",
    );
    const hasPendingDeliveries =
      (yield* sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'scheduled_task_webhook_pending'`)
        .length > 0;
    const hasReplayKeys =
      (yield* sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'scheduled_task_webhook_replay_keys'`)
        .length > 0;
    const deferredTaskIds = () =>
      hasPendingDeliveries
        ? sql<{
            task_id: string;
          }>`SELECT DISTINCT task_id FROM scheduled_task_webhook_pending `.pipe(
            Effect.map((rows) => new Set(rows.map((row) => row.task_id))),
          )
        : Effect.succeed(new Set<string>());
    const checkDeferredTask = Effect.fnUntraced(
      function* (id: ScheduledTaskId) {
        if ((yield* deferredTaskIds()).has(id))
          return yield* taskError("Newer webhook work is deferred until roll-forward.", {
            taskId: id,
          });
      },
      Effect.mapError((cause) =>
        isScheduledTaskError(cause)
          ? cause
          : taskError("Could not check deferred webhook work.", { cause }),
      ),
    );
    const rawObject = Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown));
    const decodeRawObject = Schema.decodeUnknownEffect(rawObject);
    const retainScheduleJson = Effect.fnUntraced(
      function* (id: ScheduledTaskId, encoded: string) {
        const [row] = yield* sql<{
          schedule_json: string;
        }>`SELECT schedule_json FROM scheduled_tasks WHERE task_id = ${id}`;
        if (row === undefined) return encoded;
        const stored = yield* decodeRawObject(row.schedule_json);
        const next = yield* decodeRawObject(encoded);
        const before = stored.type === "destination-automation-v1" ? stored.schedule : stored;
        const after = next.type === "destination-automation-v1" ? next.schedule : next;
        const schedule = yield* Effect.try({
          try: () =>
            retainNewerFields(
              SchemaAST.toEncoded(ScheduledTask.fields.schedule.ast),
              before,
              after,
            ),
          catch: (cause) =>
            taskError("Could not retain newer schedule settings.", { taskId: id, cause }),
        });
        return yield* encodeAutomationStamp(
          next.type === "destination-automation-v1" ? { ...next, schedule } : schedule,
        );
      },
      Effect.mapError((cause) => taskError("Could not retain newer schedule settings.", { cause })),
    );

    yield* MutationReceipts.schema.pipe(Effect.orDie);
    const nativeReceipts = yield* Effect.serviceOption(CommandReceipts.CommandReceiptStoreV2);
    // New authenticated admissions establish prospective ownership for imported
    // tasks. A separate incarnation fences even same-id, same-timestamp recreation.
    yield* sql`CREATE TABLE IF NOT EXISTS scheduled_task_request_incarnations (task_id TEXT PRIMARY KEY, incarnation TEXT NOT NULL, active_request_id TEXT)`;
    yield* sql`CREATE TRIGGER IF NOT EXISTS scheduled_task_request_created AFTER INSERT ON scheduled_tasks BEGIN INSERT OR REPLACE INTO scheduled_task_request_incarnations VALUES (new.task_id,lower(hex(randomblob(16))),NULL); END`;
    yield* sql`CREATE TRIGGER IF NOT EXISTS scheduled_task_request_deleted AFTER DELETE ON scheduled_tasks BEGIN DELETE FROM scheduled_task_request_incarnations WHERE task_id=old.task_id; END`;
    yield* sql`CREATE TABLE IF NOT EXISTS scheduled_manual_requests (request_id TEXT PRIMARY KEY, generation TEXT NOT NULL, input_digest TEXT NOT NULL, task_id TEXT NOT NULL, incarnation TEXT NOT NULL, snapshot_json TEXT NOT NULL, started_at TEXT NOT NULL, stage TEXT NOT NULL, reference_json TEXT, result_json TEXT)`;

    const bareLaunch = yield* BareLaunch.BareLaunch;
    const threadLaunch = yield* ThreadLaunchService.ThreadLaunchService;
    const threadManagement = yield* ThreadManagementService.ThreadManagementService;
    const secretRequests = yield* SecretRequests.SecretRequests;
    const scheduler = yield* Scheduler.Scheduler;
    const readWebhookOrigin = yield* ScheduledTaskWebhookOrigin;
    // Fork: outcome checks and command tasks (toolboxmd/chromeria#174).
    const dispatchPolicy = yield* ScheduledTaskDispatchPolicy;
    // Webhook deliveries for one task dispatch in arrival order rather than
    // being dropped while an earlier delivery is still dispatching.
    const webhookPermits = yield* Ref.make<ReadonlyMap<ScheduledTaskId, Semaphore.Semaphore>>(
      new Map(),
    );
    // Keyed by task id and creation time, so deliveries of a deleted task that
    // finish late release their own count, never a recreated task's.
    const webhookQueued = yield* Ref.make<ReadonlyMap<string, number>>(new Map());
    const webhookRateWindows = yield* Ref.make<ReadonlyMap<ScheduledTaskId, RateWindow>>(new Map());
    const activeRuns = yield* Ref.make<ReadonlySet<ScheduledTaskId>>(new Set());
    // Sliding(1) coalesces the dirty-signal: every notification triggers a
    // full list() re-emit anyway, so a slow subscriber only ever needs the
    // latest signal — an unbounded backlog would just grow memory.
    const changesPubSub = yield* PubSub.sliding<void>(1);
    const notifyChanged = PubSub.publish(changesPubSub, undefined).pipe(Effect.asVoid);

    const selectAllRows = () => sql<ScheduledTaskRow>`
      SELECT
        task_id,
        title,
        prompt,
        enabled,
        schedule_json,
        purpose_intent_json,
        project_id,
        thread_id,
        workspace_strategy_json,
        model_selection_json,
        runtime_mode,
        interaction_mode,
        created_by,
        creation_source,
        bare_json,
        link_origin_json,
        created_at,
        updated_at,
        next_run_at,
        last_run_at,
        last_run_status,
        last_run_error,
        run_count,
        webhook_token,
        webhook_secret
      FROM scheduled_tasks
      ORDER BY updated_at DESC, task_id ASC
    `;

    const listRows = Effect.fn("ScheduledTaskService.listRows")(function* () {
      const rows = yield* selectAllRows();
      const origin = yield* readWebhookOrigin;
      return yield* Effect.forEach(rows, (row) => decodeRow(row, origin), { concurrency: 1 });
    });

    const getRows = (id: ScheduledTaskId) => sql<ScheduledTaskRow>`
      SELECT
        task_id,
        title,
        prompt,
        enabled,
        schedule_json,
        purpose_intent_json,
        project_id,
        thread_id,
        workspace_strategy_json,
        model_selection_json,
        runtime_mode,
        interaction_mode,
        created_by,
        creation_source,
        bare_json,
        link_origin_json,
        created_at,
        updated_at,
        next_run_at,
        last_run_at,
        last_run_status,
        last_run_error,
        run_count,
        webhook_token,
        webhook_secret
      FROM scheduled_tasks
      WHERE task_id = ${id}
    `;

    const findTask = Effect.fn("ScheduledTaskService.findTask")(function* (id: ScheduledTaskId) {
      const rows = yield* getRows(id).pipe(
        Effect.mapError((cause) =>
          taskError("Could not load schedule task.", { taskId: id, cause }),
        ),
      );
      const row = rows[0];
      if (row === undefined) return null;
      return yield* decodeRow(row, yield* readWebhookOrigin);
    });

    const findWebhookCredentials = (id: ScheduledTaskId) =>
      getRows(id).pipe(
        Effect.map((rows) =>
          rows[0] === undefined
            ? null
            : { token: rows[0].webhook_token, secret: rows[0].webhook_secret },
        ),
        Effect.mapError((cause) =>
          taskError("Could not load schedule task.", { taskId: id, cause }),
        ),
      );

    const newWebhookToken = crypto.randomBytes(32).pipe(
      Effect.map((bytes) => Buffer.from(bytes).toString("base64url")),
      Effect.mapError((cause) => taskError("Could not generate webhook token.", { cause })),
    );

    const loadTask = Effect.fn("ScheduledTaskService.loadTask")(function* (id: ScheduledTaskId) {
      const task = yield* findTask(id);
      if (task === null) {
        return yield* taskError("Schedule task not found.", { taskId: id });
      }
      return task;
    });

    // Run-state columns (last_run_*, run_count) are intentionally absent from
    // the conflict clause: they are owned by the run transitions below, and a
    // concurrent settings save must not overwrite an in-flight increment.
    // Check existence in the write itself so an edit cannot undo a deletion
    // that landed after upsert loaded the previous task.
    const saveTask = (
      task: ScheduledTask,
      requireExisting: boolean,
      webhook: {
        readonly token: string | null;
        readonly secret: string | null;
        readonly secretChanged: boolean;
      },
      bareReadGrant: BareReadGrant | undefined,
    ) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const scheduleJson = yield* retainScheduleJson(
              task.id,
              yield* encodeAutomationStamp(task.schedule),
            );
            return yield* sql<{ task_id: string }>`
        INSERT INTO scheduled_tasks (
          task_id,
          title,
          prompt,
          enabled,
          schedule_json,
        purpose_intent_json,
          project_id,
          thread_id,
          workspace_strategy_json,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          created_by,
          creation_source,
          bare_json,
        link_origin_json,
          created_at,
          updated_at,
          next_run_at,
          last_run_at,
          last_run_status,
          last_run_error,
          run_count,
          webhook_token,
          webhook_secret
        )
        SELECT
          ${task.id},
          ${task.title},
          ${task.prompt},
          ${task.enabled ? 1 : 0},
          ${scheduleJson},
          ${task.purposeIntent === undefined ? null : encodePurposeIntentJson(task.purposeIntent)},
          ${task.projectId},
          ${task.threadId},
          ${yield* encodeAutomationStamp(task.workspaceStrategy)},
          ${yield* encodeAutomationStamp(task.modelSelection)},
          ${task.runtimeMode},
          ${task.interactionMode},
          ${task.createdBy},
          ${task.creationSource},
          ${task.bare === undefined || bareReadGrant === undefined ? null : encodeScheduledBare({ request: task.bare, readGrant: bareReadGrant })},
          ${task.linkOrigin === undefined ? null : yield* encodeAutomationStamp(task.linkOrigin)},
          ${task.createdAt},
          ${task.updatedAt},
          ${task.nextRunAt},
          ${task.lastRunAt},
          ${task.lastRunStatus},
          ${task.lastRunError},
          ${task.runCount},
          ${webhook.token},
          ${webhook.secret}
        WHERE ${requireExisting ? 0 : 1} = 1
           OR EXISTS (SELECT 1 FROM scheduled_tasks WHERE task_id = ${task.id})
        ON CONFLICT (task_id)
        DO UPDATE SET
          bare_json = excluded.bare_json,
          title = excluded.title,
          prompt = excluded.prompt,
          enabled = excluded.enabled,
          schedule_json = excluded.schedule_json,
          purpose_intent_json = excluded.purpose_intent_json,
          project_id = excluded.project_id,
          thread_id = excluded.thread_id,
          workspace_strategy_json = excluded.workspace_strategy_json,
          model_selection_json = excluded.model_selection_json,
          runtime_mode = excluded.runtime_mode,
          interaction_mode = excluded.interaction_mode,
          creation_source = excluded.creation_source,
          updated_at = excluded.updated_at,
          next_run_at = excluded.next_run_at,
          -- Only rotate changes a live token, so a save racing a rotation
          -- cannot bring the old URL back.
          webhook_token = CASE
            WHEN excluded.webhook_token IS NULL THEN NULL
            ELSE COALESCE(scheduled_tasks.webhook_token, excluded.webhook_token)
          END,
          webhook_secret = CASE
            WHEN ${webhook.secretChanged ? 1 : 0} = 1 THEN excluded.webhook_secret
            ELSE scheduled_tasks.webhook_secret
          END
        RETURNING task_id
      `.pipe(
              Effect.mapError((cause) =>
                taskError("Could not save schedule task.", { taskId: task.id, cause }),
              ),
              Effect.flatMap((rows) =>
                rows.length > 0
                  ? Effect.void
                  : taskError("Schedule task not found.", { taskId: task.id }),
              ),
            );
          }),
        )
        .pipe(
          Effect.mapError((cause) =>
            isScheduledTaskError(cause)
              ? cause
              : taskError("Could not save schedule task.", { taskId: task.id, cause }),
          ),
        );

    const deleteRow = (id: ScheduledTaskId) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            yield* checkDeferredTask(id);
            if (
              hasReplayKeys &&
              (yield* sql`SELECT 1 FROM scheduled_task_webhook_replay_keys WHERE task_id = ${id} LIMIT 1`)
                .length > 0
            )
              return yield* taskError(
                "Newer webhook replay state is retained until roll-forward; disable this task instead.",
                { taskId: id },
              );
            if (
              (yield* sql`SELECT 1 FROM scheduled_task_webhook_deliveries WHERE task_id = ${id} LIMIT 1`)
                .length > 0
            )
              return yield* taskError(
                "Delivery history is retained until roll-forward; disable this task instead.",
                { taskId: id },
              );
            yield* sql`DELETE FROM scheduled_tasks WHERE task_id = ${id}`;
          }),
        )
        .pipe(
          Effect.mapError((cause) =>
            taskError("Could not delete schedule task.", { taskId: id, cause }),
          ),
        );

    const markRunning = (id: ScheduledTaskId, startedAtIso: string, deliveryId?: string) =>
      sql.withTransaction(
        checkDeferredTask(id).pipe(
          Effect.andThen(
            sql<{ task_id: string }>`
        UPDATE scheduled_tasks
        SET updated_at = ${startedAtIso},
            last_run_at = ${startedAtIso},
            last_run_status = 'running',
            last_run_error = NULL
            ${hasDeliveryCorrelation ? sql`, last_run_webhook_delivery_id = ${deliveryId ?? null}` : sql``}
        WHERE task_id = ${id}
        RETURNING task_id
      `.pipe(
              Effect.mapError((cause) =>
                taskError("Could not mark schedule task as running.", { taskId: id, cause }),
              ),
              // A task deleted after the re-read must not be dispatched from the stale snapshot.
              Effect.flatMap((rows) =>
                rows.length > 0
                  ? sql`UPDATE scheduled_task_request_incarnations SET active_request_id=NULL WHERE task_id=${id}`.pipe(
                      Effect.asVoid,
                      Effect.mapError((cause) =>
                        taskError("Could not bind schedule run.", { taskId: id, cause }),
                      ),
                    )
                  : taskError("Schedule task not found.", { taskId: id }),
              ),
            ),
          ),
        ),
      );

    const markCompleted = (input: {
      readonly id: ScheduledTaskId;
      readonly completedAtIso: string;
      readonly nextRunAtIso: string | null;
      readonly status: "succeeded" | "failed";
      readonly error: string | null;
      readonly startedAtIso: string;
    }) =>
      sql`
        UPDATE scheduled_tasks
        SET updated_at = ${input.completedAtIso},
            next_run_at = ${input.nextRunAtIso},
            last_run_status = ${input.status},
            last_run_error = ${input.error},
            run_count = run_count + 1
        WHERE task_id = ${input.id}
          AND last_run_status = 'running'
          AND last_run_at = ${input.startedAtIso}
      `.pipe(
        Effect.mapError((cause) =>
          taskError("Could not record schedule task run.", { taskId: input.id, cause }),
        ),
      );

    // Best-effort escape hatch: if anything fails between markRunning and
    // markCompleted, write a full terminal record so runDueTasks neither skips
    // the task forever (it filters out 'running' rows) nor re-fires it
    // immediately: the dispatch may already have gone out, so next_run_at must
    // advance and run_count must count the attempt.
    const releaseStuckRun = (task: ScheduledTask, message: string) =>
      Effect.gen(function* () {
        const now = yield* localNow;
        const reread = yield* Effect.result(findTask(task.id));
        if (Result.isSuccess(reread) && reread.success === null) return;
        const source = Result.isSuccess(reread) && reread.success !== null ? reread.success : task;
        yield* sql`
          UPDATE scheduled_tasks
          SET last_run_status = 'failed',
              last_run_error = ${message},
              next_run_at = ${nextRunAt(source, now)},
              updated_at = ${iso(now)},
              run_count = run_count + 1
          WHERE task_id = ${task.id} AND last_run_status = 'running'
        `;
        yield* notifyChanged;
      }).pipe(
        Effect.catch((cause) =>
          Effect.logWarning("Could not release stuck schedule task run", {
            taskId: task.id,
            cause,
          }),
        ),
      );

    const validateBinding = (
      task: Pick<ScheduledTask, "id" | "threadId" | "linkOrigin" | "bare">,
      liveAcceptance = false,
    ) =>
      Effect.gen(function* () {
        if (task.bare !== undefined && task.threadId !== null) {
          const target = yield* threadManagement
            .getThreadRecords(task.threadId, [])
            .pipe(
              Effect.mapError((cause) =>
                taskError("Could not read the scheduled thread.", { taskId: task.id, cause }),
              ),
            );
          if (target.thread.bare === undefined || (task.bare.additions?.length ?? 0) > 0)
            return yield* taskError(
              "A bound bare schedule needs a bare thread and no new additions.",
              { taskId: task.id },
            );
        }

        const accepted = yield* automation.current("schedule", task.id);
        if (Option.isSome(accepted) && !liveAcceptance) {
          yield* automation.claim(accepted.value.id);
          if (task.threadId !== accepted.value.binding.threadId)
            return yield* taskError("The accepted schedule binding changed.", { taskId: task.id });
        }
        if (
          !liveAcceptance &&
          Option.isNone(accepted) &&
          task.linkOrigin !== undefined &&
          task.threadId !== null
        ) {
          const target = yield* threadManagement
            .getThreadShell(task.threadId)
            .pipe(
              Effect.mapError((cause) =>
                taskError("Could not read the scheduled thread.", { taskId: task.id, cause }),
              ),
            );
          if (target?.linkOrigin?.sessionId !== task.linkOrigin.sessionId)
            return yield* taskError(
              "A linked scheduled task may run only in a thread from its own link.",
              { taskId: task.id },
            );
        }
      });

    const dispatchTask = (
      active: ScheduledTask,
      commandId: CommandId,
      messageId: MessageId,
      prompt: string,
      decision: import("../scheduledTaskChecks/DispatchPolicy.ts").ScheduledTaskDispatchDecision,
      stableThreadId?: ThreadId,
      bareReadGrantOverride?: BareReadGrant,
      acceptedAutomation?: { readonly certificateId: string | null },
    ) =>
      Effect.gen(function* () {
        const acceptance = yield* automation.current("schedule", active.id);
        if (acceptedAutomation?.certificateId === null && Option.isSome(acceptance))
          return yield* taskError(
            "An unstamped historical run cannot borrow a newer automation definition.",
            { taskId: active.id },
          );
        const certificateId =
          acceptedAutomation === undefined
            ? Option.getOrUndefined(acceptance)?.id
            : acceptedAutomation.certificateId;
        if (certificateId !== null && certificateId !== undefined) {
          if (Option.isNone(peerAuthority))
            return yield* taskError("Native automation execution is unavailable.", {
              taskId: active.id,
            });
          const execution = yield* peerAuthority.value.automationExecution(
            certificateId,
            commandId,
          );
          return yield* dispatchOwnedTask(
            active,
            commandId,
            messageId,
            prompt,
            decision,
            stableThreadId,
            bareReadGrantOverride,
          ).pipe(Effect.provideService(PeerAcceptedAutomation, execution.id));
        }
        return yield* dispatchOwnedTask(
          active,
          commandId,
          messageId,
          prompt,
          decision,
          stableThreadId,
          bareReadGrantOverride,
        );
      });

    const dispatchOwnedTask = (
      active: ScheduledTask,
      commandId: CommandId,
      messageId: MessageId,
      prompt: string,
      decision: import("../scheduledTaskChecks/DispatchPolicy.ts").ScheduledTaskDispatchDecision,
      stableThreadId?: ThreadId,
      bareReadGrantOverride?: BareReadGrant,
    ) =>
      Effect.gen(function* () {
        const bareAuthority =
          active.bare === undefined
            ? undefined
            : bareReadGrantOverride !== undefined
              ? { readGrant: bareReadGrantOverride }
              : yield* readScheduledBare(active.id).pipe(
                  Effect.provideService(SqlClient.SqlClient, sql),
                  Effect.mapError((cause) =>
                    taskError("Could not read bare schedule authority.", {
                      taskId: active.id,
                      cause,
                    }),
                  ),
                );
        const resolvedPurpose = yield* PurposeAdmission.resolvePurposeIntent({
          intent: active.purposeIntent,
        }).pipe(
          Effect.mapError((cause) =>
            taskError("Could not resolve scheduled task purpose.", { taskId: active.id, cause }),
          ),
        );
        const purposeSource: ExecutionPurpose =
          active.purposeIntent === undefined
            ? resolvedPurpose.purpose
            : {
                version: 1,
                scope: resolvedPurpose.purpose.scope,
                provenance: { type: "schedule", scheduledTaskId: active.id },
              };
        return active.threadId === null
          ? yield* Effect.exit(
              dispatchVia(
                decision,
                threadLaunch.launch,
              )({
                commandId,
                purposeSource,
                ...(stableThreadId === undefined ? {} : { threadId: stableThreadId }),
                ...(active.bare === undefined
                  ? {}
                  : {
                      bare: active.bare,
                      ...(bareAuthority === undefined
                        ? {}
                        : { bareReadGrant: bareAuthority.readGrant }),
                    }),
                projectId: active.projectId,
                title: active.title,
                modelSelection: active.modelSelection,
                runtimeMode: active.runtimeMode,
                interactionMode: active.interactionMode,
                workspaceStrategy: active.workspaceStrategy,
                initialMessage: {
                  messageId,
                  scheduledTaskId: active.id,
                  text: prompt,
                  attachments: [],
                },
                createdBy: active.createdBy,
                creationSource: active.creationSource,
                ...(active.linkOrigin === undefined ? {} : { linkOrigin: active.linkOrigin }),
              }),
            )
          : yield* Effect.exit(
              dispatchVia(
                decision,
                threadManagement.sendToThread,
              )({
                projectId: active.projectId,
                commandId,
                purposeSource,
                threadId: ThreadId.make(active.threadId),
                messageId,
                scheduledTaskId: active.id,
                text: prompt,
                attachments: [],
                modelSelection: active.modelSelection,
                // Scheduled prompts must not interrupt tools in the bound thread.
                mode: "queue",
                createdBy: active.createdBy,
                creationSource: active.creationSource,
              }),
            );
      });

    const runTask = Effect.fn("ScheduledTaskService.runTask")(function* (
      task: ScheduledTask,
      trigger: "scheduled" | "manual" | "webhook",
      webhook?: { readonly deliveryId: string; readonly prompt: string },
    ) {
      const reserved = yield* Ref.modify(activeRuns, (active) => {
        if (active.has(task.id)) return [false, active] as const;
        const next = new Set(active);
        next.add(task.id);
        return [true, next] as const;
      });
      if (!reserved) {
        if (trigger !== "scheduled") {
          return yield* taskError("Schedule task is already running.", { taskId: task.id });
        }
        return task;
      }

      return yield* Effect.gen(function* () {
        const startedAt = yield* localNow;
        const startedAtIso = iso(startedAt);

        // The in-memory snapshot may be stale: re-read before touching run
        // state. The task may have been deleted, paused, or postponed since
        // the poll loaded it — none of those may fire.
        const active = yield* findTask(task.id);
        if (active === null) {
          if (webhook !== undefined) {
            return yield* new WebhookDeliverySkipped({
              reason: "The task was deleted before this delivery ran.",
            });
          }
          if (trigger !== "scheduled") {
            return yield* taskError("Schedule task not found.", { taskId: task.id });
          }
          return task;
        }
        const parsedNextRunAt =
          active.nextRunAt === null ? Option.none() : DateTime.make(active.nextRunAt);
        if (
          trigger === "scheduled" &&
          (!active.enabled ||
            Option.isNone(parsedNextRunAt) ||
            DateTime.toEpochMillis(parsedNextRunAt.value) > DateTime.toEpochMillis(startedAt))
        ) {
          return active;
        }
        // A queued delivery must not run a task that was paused, deleted and
        // recreated under the same id, or switched to another trigger while
        // it waited for its turn.
        if (webhook !== undefined) {
          const reason =
            active.createdAt !== task.createdAt
              ? "The task was replaced before this delivery ran."
              : active.schedule.type !== "webhook"
                ? "The task's trigger changed before this delivery ran."
                : !active.enabled
                  ? "The task was paused before this delivery ran."
                  : null;
          if (reason !== null) return yield* new WebhookDeliverySkipped({ reason });
        }
        yield* validateBinding(active);
        if (trigger === "manual") {
          const scope = yield* Effect.serviceOption(Invocation.McpInvocationContext);
          if (Option.isSome(scope) && scope.value.authority !== undefined) {
            if (Option.isNone(peerAuthority))
              return yield* taskError("Live manual-run admission is unavailable.", {
                taskId: active.id,
              });
            yield* peerAuthority.value
              .checkInteractiveRecord(scope.value.authority.id)
              .pipe(
                Effect.mapError((cause) => taskError(cause.message, { taskId: active.id, cause })),
              );
          }
        }
        // Fork: decided before the task is marked running; a skip consumed the slot.
        const decision = yield* dispatchPolicy
          .decide({ task: active, trigger, startedAt })
          .pipe(Effect.mapError((error) => taskError(error.message, { taskId: active.id })));
        if (decision._tag === "skip") return active;

        yield* markRunning(active.id, startedAtIso, webhook?.deliveryId);
        yield* notifyChanged;

        const fireKey =
          webhook === undefined
            ? (forkFireKey(active, trigger) ??
              `${active.id}:${DateTime.toEpochMillis(startedAt)}:${trigger}`)
            : `${active.id}:webhook:${webhook.deliveryId}`;
        const commandId = CommandId.make(`scheduled-task:${fireKey}`);
        const messageId = MessageId.make(`scheduled-task-message:${fireKey}`);
        // A webhook prompt was rendered from the row when the request arrived.
        const prompt = webhook?.prompt ?? active.prompt;

        // Effect.exit (not Effect.result) so defects and interruptions in the
        // dispatch are also captured and recorded as a failed run instead of
        // aborting before markCompleted.
        const result = yield* dispatchTask(active, commandId, messageId, prompt, decision);

        const completedAt = yield* localNow;
        const runSucceeded = result._tag === "Success";
        const lastRunStatus = runSucceeded ? ("succeeded" as const) : ("failed" as const);
        const lastRunError = runSucceeded ? null : errorMessage(result.cause);
        const current = yield* findTask(task.id);
        const scheduleSource = current ?? task;
        const completed: ScheduledTask = {
          ...scheduleSource,
          updatedAt: iso(completedAt),
          lastRunAt: startedAtIso,
          nextRunAt: nextRunAt(scheduleSource, completedAt),
          lastRunStatus,
          lastRunError,
          runCount: scheduleSource.runCount + 1,
        };
        if (current !== null) {
          // startedAtIso in the guard ensures this writes only to the row this
          // run marked as running — a task deleted mid-run and recreated with
          // the same id (idempotent commandId replay) must not be stamped.
          yield* markCompleted({
            id: task.id,
            completedAtIso: completed.updatedAt,
            nextRunAtIso: completed.nextRunAt,
            status: lastRunStatus,
            error: lastRunError,
            startedAtIso,
          });
          yield* notifyChanged;
        }
        return completed;
      }).pipe(
        Effect.onError((cause) => releaseStuckRun(task, errorMessage(cause))),
        Effect.ensuring(
          Ref.update(activeRuns, (active) => {
            const next = new Set(active);
            next.delete(task.id);
            return next;
          }),
        ),
      );
    });

    const rescheduleMissedRun = Effect.fn("ScheduledTaskService.rescheduleMissedRun")(function* (
      task: ScheduledTask,
      now: DateTime.DateTime,
    ) {
      const next = nextRunAt(task, now);
      yield* Effect.logInfo("Skipping missed schedule task run", {
        taskId: task.id,
        missedRunAt: task.nextRunAt,
        rescheduledTo: next,
      });
      yield* sql`
        UPDATE scheduled_tasks
        SET next_run_at = ${next},
            updated_at = ${iso(now)}
        WHERE task_id = ${task.id}
      `.pipe(
        Effect.mapError((cause) =>
          taskError("Could not reschedule missed schedule task run.", { taskId: task.id, cause }),
        ),
      );
      yield* notifyChanged;
    });

    const runDueTasks = Effect.fn("ScheduledTaskService.runDueTasks")(function* () {
      const now = yield* localNow;
      const tasks = yield* listDueTasks(now).pipe(
        Effect.mapError((cause) => taskError("Could not list schedule tasks.", { cause })),
      );
      const due = tasks.flatMap((task) =>
        task.nextRunAt === null ? [] : [{ task, dueAt: DateTime.makeUnsafe(task.nextRunAt) }],
      );
      yield* Effect.forEach(
        due,
        ({ task, dueAt }) =>
          Effect.suspend(() =>
            isMissedFixedTimeRun(task.schedule, dueAt, now)
              ? rescheduleMissedRun(task, now)
              : runTask(task, "scheduled"),
          ).pipe(
            Effect.catch((cause) =>
              Effect.logWarning("Scheduled task run failed", { taskId: task.id, cause }),
            ),
          ),
        { concurrency: 1, discard: true },
      );
    });

    // Recover from a crash or hard shutdown mid-run: rows stuck in 'running'
    // would otherwise be skipped by the due-task filter forever.
    yield* Effect.gen(function* () {
      const rows = yield* selectAllRows();
      // Keyed manual admissions have an exact native receipt recovery path.
      // Generic restart recovery must not count that same action first.
      const keyed = new Set(
        (yield* sql<{
          task_id: string;
        }>`SELECT i.task_id FROM scheduled_task_request_incarnations i JOIN scheduled_manual_requests r ON r.request_id=i.active_request_id AND r.incarnation=i.incarnation WHERE r.result_json IS NULL`).map(
          (row) => row.task_id,
        ),
      );
      const deferred = yield* deferredTaskIds();
      if (deferred.size > 0)
        yield* Effect.logWarning("Newer pending webhook work deferred until roll-forward", {
          taskIds: [...deferred],
        });
      const stuck = rows.filter(
        (row) =>
          row.last_run_status === "running" &&
          !keyed.has(row.task_id ?? "") &&
          !deferred.has(row.task_id ?? ""),
      );
      if (stuck.length === 0) return;
      const now = yield* localNow;
      yield* Effect.forEach(
        stuck,
        (row) =>
          Effect.gen(function* () {
            const decoded = yield* Effect.result(decodeRow(row));
            if (Result.isSuccess(decoded)) {
              yield* sql`
                UPDATE scheduled_tasks
                SET last_run_status = 'failed',
                    last_run_error = 'Run was interrupted by a server restart.',
                    next_run_at = ${nextRunAt(decoded.success, now)},
                    updated_at = ${iso(now)},
                    run_count = run_count + 1
                WHERE task_id IS ${row.task_id} AND last_run_status = 'running'
              `;
              return;
            }
            yield* Effect.logWarning(
              "Recovering undecodable schedule task row without rescheduling",
              { taskId: row.task_id, cause: decoded.failure },
            );
            yield* sql`
              UPDATE scheduled_tasks
              SET last_run_status = 'failed',
                  last_run_error = 'Run was interrupted by a server restart.',
                  updated_at = ${iso(now)},
                  run_count = run_count + 1
              WHERE task_id IS ${row.task_id} AND last_run_status = 'running'
            `;
          }),
        { concurrency: 1, discard: true },
      );
      yield* notifyChanged;
    }).pipe(
      Effect.catch((cause) =>
        Effect.logWarning("Could not reset interrupted schedule task runs", { cause }),
      ),
    );

    yield* scheduler.register("scheduled-tasks", runDueTasks());

    const list: ScheduledTaskService["Service"]["list"] = () =>
      listRows().pipe(
        Effect.map((tasks) => ({ tasks })),
        Effect.mapError((cause) => taskError("Could not list schedule tasks.", { cause })),
      );

    const subscribeList: ScheduledTaskService["Service"]["subscribeList"] = () =>
      Stream.unwrap(
        Effect.gen(function* () {
          // Subscribe before taking the snapshot so a change landing between
          // the two is buffered by the subscription rather than dropped.
          const subscription = yield* PubSub.subscribe(changesPubSub);
          return Stream.concat(
            Stream.fromEffect(list()),
            Stream.fromSubscription(subscription).pipe(Stream.mapEffect(() => list())),
          );
        }),
      );

    const upsert: ScheduledTaskService["Service"]["upsert"] = (input, options) =>
      Effect.gen(function* () {
        if (input.purposeIntent === undefined) {
          return yield* taskError(
            "Choose an Issue, PR or deliberately unscoped purpose before saving this schedule.",
          );
        }
        const mutation = yield* MutationReceipts.CurrentMutation;
        if (mutation !== undefined) {
          const receipt = yield* MutationReceipts.confirmed(mutation.request).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          );
          if (Option.isSome(receipt))
            return { task: yield* loadTask(receipt.value.scheduledTaskId) };
        }
        const now = yield* localNow;
        const uuid =
          input.commandId === undefined
            ? yield* crypto
                .randomUUIDv4()
                .pipe(
                  Effect.mapError((cause) =>
                    taskError("Could not generate schedule task id.", { cause }),
                  ),
                )
            : null;
        const id =
          input.id ??
          ScheduledTaskId.make(
            input.commandId ? `scheduled-task:${input.commandId}` : `scheduled-task:${uuid}`,
          );
        if (yield* MutationReceipts.InAcceptance)
          return yield* taskError("Schedule capture must finish before native acceptance.", {
            taskId: id,
          });
        const localFence = yield* MutationReceipts.captureResourceFence(id, input.threadId).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        const extension = yield* MutationReceipts.DefinitionExtension;
        const scope = yield* Effect.serviceOption(Invocation.McpInvocationContext);
        if (
          Option.isNone(peerAuthority) &&
          Option.isSome(scope) &&
          scope.value.authority !== undefined
        )
          return yield* taskError("Destination automation authority is unavailable.", {
            taskId: id,
          });
        const definition = {
          ...input,
          id,
          commandId: undefined,
          requireExisting: undefined,
          extension: extension?.definition,
        };
        const nativeActionId = input.commandId ?? `scheduled-task:${uuid}`;
        const binding: Automation.AutomationBinding = {
          kind: "schedule",
          resourceId: id,
          projectId: input.projectId,
          threadId: input.threadId ?? null,
          definitionGeneration: mutation?.request.id ?? nativeActionId,
          definitionDigest: requestDigest(definition),
          incarnation: null,
        };
        const accepted = Option.isNone(peerAuthority)
          ? undefined
          : yield* peerAuthority.value.captureAutomation(
              Option.getOrUndefined(scope),
              binding,
              mutation?.request ?? { nativeCommandId: nativeActionId },
              {
                runtimeMode: input.runtimeMode,
                interactionMode: input.interactionMode,
                resourceScope: "environment-resources",
                onward: true,
                capabilities: Option.isSome(scope)
                  ? [...scope.value.capabilities]
                  : ["orchestration", "worktree", "pull-requests", "preview", "device"],
              },
            );
        const extensionCommit = extension === undefined ? undefined : yield* extension.prepare;
        return yield* MutationReceipts.accept(
          id,
          sql.withTransaction(
            Effect.gen(function* () {
              // Look up by the *resolved* id so idempotent creates (commandId replays)
              // keep their run history, and so real load failures propagate instead
              // of silently resetting an existing row.
              yield* localFence;
              if (extensionCommit !== undefined) yield* extensionCommit;
              const existingTask = yield* findTask(id);
              const bare = input.bare ?? existingTask?.bare;
              const previousBare =
                existingTask?.bare === undefined
                  ? undefined
                  : yield* readScheduledBare(id).pipe(
                      Effect.provideService(SqlClient.SqlClient, sql),
                      Effect.mapError((cause) =>
                        taskError("Could not read bare schedule authority.", { taskId: id, cause }),
                      ),
                    );
              const bareReadGrant =
                bare === undefined
                  ? undefined
                  : ((input.bare === undefined
                      ? previousBare?.readGrant
                      : options?.bareReadGrant) ??
                    previousBare?.readGrant ??
                    (input.runtimeMode === "full-access"
                      ? { type: "all" as const }
                      : { type: "roots" as const, roots: [] }));
              if (bare !== undefined)
                yield* bareLaunch
                  .validate({
                    request: bare,
                    instanceId: input.modelSelection.instanceId,
                    readGrant: bareReadGrant,
                  })
                  .pipe(Effect.mapError((cause) => taskError(cause.message, { cause })));
              const linkOrigin = existingTask === null ? input.linkOrigin : existingTask.linkOrigin;
              yield* validateBinding(
                {
                  ...(bare === undefined ? {} : { bare }),
                  id,
                  threadId: input.threadId ?? null,
                  ...(linkOrigin === undefined ? {} : { linkOrigin }),
                },
                accepted !== undefined,
              );
              // Keep the existing next_run_at when the schedule itself is untouched:
              // editing a title or prompt must not postpone (or resurrect) a due
              // run — only schedule/enabled changes restart the clock.
              const schedule: ScheduledTask["schedule"] =
                input.schedule.type === "webhook"
                  ? {
                      type: "webhook",
                      signature:
                        input.schedule.signature == null
                          ? null
                          : {
                              header: input.schedule.signature.header.toLowerCase(),
                              encoding: input.schedule.signature.encoding,
                              prefix: input.schedule.signature.prefix,
                            },
                      maxDeliveryAgeMinutes: input.schedule.maxDeliveryAgeMinutes ?? null,
                    }
                  : input.schedule;
              const webhook =
                input.schedule.type === "webhook"
                  ? yield* Effect.gen(function* () {
                      const existing =
                        existingTask === null ? null : yield* findWebhookCredentials(id);
                      // Saving a webhook task keeps its URL; only rotate changes it.
                      const token = existing?.token ?? (yield* newWebhookToken);
                      const signature =
                        input.schedule.type === "webhook" ? input.schedule.signature : null;
                      // A secretRef is a value the user entered for an agent; this
                      // save consumes it, so it cannot be used again. A replay of a
                      // save that already used it keeps the secret that save stored.
                      const replay =
                        mutation === undefined &&
                        input.commandId !== undefined &&
                        existing?.secret != null;
                      const fromRef =
                        signature?.secretRef === undefined
                          ? undefined
                          : yield* secretRequests
                              .consume({ ref: signature.secretRef, projectId: input.projectId })
                              .pipe(
                                Effect.catch((error) =>
                                  replay
                                    ? Effect.succeed(undefined)
                                    : Effect.fail(taskError(error.message, { taskId: id })),
                                ),
                              );
                      // A ref, when given, is the only source: a plain secret sent
                      // alongside it must not replace what a replayed save stored.
                      const remoteOwner = yield* Effect.serviceOption(
                        RemoteSecretRequests.RemoteSecretRequests,
                      );
                      const fromRemote =
                        signature?.secretRequest === undefined
                          ? undefined
                          : mutation?.secret.type !== "remote" || Option.isNone(remoteOwner)
                            ? yield* taskError(
                                "Remote secret consumption requires its verified acceptance owner.",
                              )
                            : yield* remoteOwner.value.take(
                                signature.secretRequest,
                                mutation.request,
                              );
                      const provided =
                        signature?.secretRequest !== undefined
                          ? fromRemote
                          : signature?.secretRef === undefined
                            ? signature?.secret
                            : fromRef;
                      const secret =
                        signature == null ? null : (provided ?? existing?.secret ?? null);
                      const secretChanged =
                        signature == null || provided !== undefined || existing === null;
                      if (signature != null && secret === null) {
                        return yield* taskError(
                          "A webhook signature check needs a signing secret.",
                          {
                            taskId: id,
                          },
                        );
                      }
                      return { token, secret, secretChanged };
                    })
                  : { token: null, secret: null, secretChanged: true };
              const scheduleUnchanged =
                existingTask !== null &&
                existingTask.enabled === input.enabled &&
                isSameSchedule(existingTask.schedule, schedule);
              const task: ScheduledTask = {
                ...(bare === undefined ? {} : { bare }),
                id,
                title: input.title,
                prompt: input.prompt,
                enabled: input.enabled,
                schedule,
                purposeIntent: input.purposeIntent,
                projectId: input.projectId,
                threadId: input.threadId ?? null,
                workspaceStrategy: input.workspaceStrategy,
                modelSelection: input.modelSelection,
                runtimeMode: input.runtimeMode,
                interactionMode: input.interactionMode,
                createdBy: existingTask?.createdBy ?? input.createdBy ?? "user",
                creationSource: input.creationSource ?? "web",
                // Origin belongs to creation, not editable forms or later caller metadata.
                ...(linkOrigin === undefined ? {} : { linkOrigin }),
                createdAt: existingTask?.createdAt ?? iso(now),
                updatedAt: iso(now),
                nextRunAt: scheduleUnchanged
                  ? existingTask.nextRunAt
                  : nextRunAt({ enabled: input.enabled, schedule }, now),
                lastRunAt: existingTask?.lastRunAt ?? null,
                lastRunStatus: existingTask?.lastRunStatus ?? "never",
                lastRunError: existingTask?.lastRunError ?? null,
                runCount: existingTask?.runCount ?? 0,
              };
              yield* saveTask(task, input.requireExisting === true, webhook, bareReadGrant);
              if (accepted !== undefined) {
                const incarnation = (yield* sql<{
                  incarnation: string;
                }>`SELECT incarnation FROM scheduled_task_request_incarnations WHERE task_id=${id}`)[0]
                  ?.incarnation;
                if (incarnation === undefined)
                  return yield* taskError("The schedule incarnation is unavailable.", {
                    taskId: id,
                  });
                yield* automation.accept(accepted, { ...binding, incarnation });
                if (extension?.accepted !== undefined)
                  yield* extension.accepted(accepted.certificate.id);
                const stamp = yield* encodeAutomationStamp({
                  type: "destination-automation-v1",
                  certificateId: accepted.certificate.id,
                  schedule: task.schedule,
                }).pipe(
                  Effect.mapError((cause) =>
                    taskError("Could not encode the accepted schedule.", { taskId: id, cause }),
                  ),
                );
                yield* sql`UPDATE scheduled_tasks SET schedule_json=${yield* retainScheduleJson(id, stamp)} WHERE task_id=${id}`;
              }
              yield* notifyChanged;
              return { task: (yield* findTask(id)) ?? task };
            }),
          ),
          loadTask(id).pipe(Effect.map((task) => ({ task }))),
        ).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.tap(() => notifyAutomationChanges),
        );
      }).pipe(
        Effect.catchTags({
          OrchestratorMcpFailure: (cause) => Effect.fail(taskError(cause.message, { cause })),
          SqlError: (cause) =>
            Effect.fail(taskError("Could not accept the schedule definition.", { cause })),
        }),
      );

    const applyEnabled = (input: {
      readonly id: ScheduledTaskId;
      readonly enabled: boolean;
    }): Effect.Effect<ScheduledTaskMutationResult, ScheduledTaskError> =>
      Effect.gen(function* () {
        const existing = yield* loadTask(input.id);
        if (existing.enabled === input.enabled) return { task: existing };
        if (input.enabled && existing.purposeIntent === undefined)
          return yield* taskError(
            "Choose an Issue, PR or deliberately unscoped purpose before enabling this legacy schedule.",
            { taskId: input.id },
          );
        const now = yield* localNow;
        const next = nextRunAt({ enabled: input.enabled, schedule: existing.schedule }, now);
        // RETURNING so a task deleted between the load and this UPDATE is a
        // visible not-found error, not a false success.
        const updated = yield* sql.withTransaction(
          Effect.gen(function* () {
            if (!input.enabled) yield* automation.disableLocal("schedule", input.id);
            return yield* sql<{ task_id: string }>`
          UPDATE scheduled_tasks
          SET enabled = ${input.enabled ? 1 : 0},
              next_run_at = ${next},
              updated_at = ${iso(now)}
          WHERE task_id = ${input.id}
          RETURNING task_id
        `.pipe(
              Effect.mapError((cause) =>
                taskError("Could not update schedule task.", { taskId: input.id, cause }),
              ),
            );
          }),
        );
        if (updated.length === 0) {
          return yield* taskError("Schedule task not found.", { taskId: input.id });
        }
        yield* notifyAutomationChanges;
        yield* notifyChanged;
        return {
          task: { ...existing, enabled: input.enabled, nextRunAt: next, updatedAt: iso(now) },
        };
      }).pipe(
        Effect.catchTags({
          OrchestratorMcpFailure: (cause) => Effect.fail(taskError(cause.message, { cause })),
          SqlError: (cause) =>
            Effect.fail(
              taskError("Could not atomically update schedule authority.", {
                taskId: input.id,
                cause,
              }),
            ),
        }),
      );

    const setEnabled: ScheduledTaskService["Service"]["setEnabled"] = (input) =>
      Effect.gen(function* () {
        const existing = yield* loadTask(input.id);
        // Re-accepting an automation records its own keyed receipt through upsert.
        const reaccept =
          input.enabled && !existing.enabled && existing.purposeIntent !== undefined
            ? Option.isSome(yield* automation.current("schedule", input.id))
            : false;
        if (reaccept) return yield* upsert({ ...existing, enabled: true });
        return yield* MutationReceipts.accept(
          input.id,
          applyEnabled(input),
          loadTask(input.id).pipe(Effect.map((task) => ({ task }))),
        ).pipe(Effect.provideService(SqlClient.SqlClient, sql));
      }).pipe(
        Effect.catchTags({
          OrchestratorMcpFailure: (cause) => Effect.fail(taskError(cause.message, { cause })),
        }),
      );

    const deleteTask: ScheduledTaskService["Service"]["delete"] = (input) =>
      MutationReceipts.accept(
        input.id,
        sql
          .withTransaction(
            automation.disableLocal("schedule", input.id).pipe(Effect.andThen(deleteRow(input.id))),
          )
          .pipe(
            Effect.andThen(
              Effect.all([
                Ref.update(webhookRateWindows, (windows) => {
                  const next = new Map(windows);
                  next.delete(input.id);
                  return next;
                }),
                Ref.update(webhookPermits, (permits) => {
                  const next = new Map(permits);
                  next.delete(input.id);
                  return next;
                }),
              ]),
            ),
            Effect.andThen(notifyChanged),
            Effect.as({ id: input.id }),
          ),
        Effect.succeed({ id: input.id }),
      ).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
        Effect.tap(() => notifyAutomationChanges),
        Effect.catchTags({
          SqlError: (cause) =>
            Effect.fail(
              taskError("Could not atomically delete schedule authority.", {
                taskId: input.id,
                cause,
              }),
            ),
          OrchestratorMcpFailure: (cause) => Effect.fail(taskError(cause.message, { cause })),
        }),
      );

    const ManualSnapshot = Schema.Struct({
      task: ScheduledTask,
      bareReadGrant: Schema.optional(BareReadGrant),
      automationCertificateId: Schema.optionalKey(Schema.String),
    });
    const encodeManualSnapshot = Schema.encodeEffect(Schema.fromJsonString(ManualSnapshot));
    const encodeManualReference = Schema.encodeEffect(
      Schema.fromJsonString(Schema.Struct({ runId: Schema.String, requestId: Schema.String })),
    );
    const encodeManualResult = Schema.encodeEffect(
      Schema.fromJsonString(
        Schema.Struct({ task: ScheduledTask, manualReceipt: McpManualRunReceipt }),
      ),
    );
    const decodeManualSnapshot = Schema.decodeUnknownEffect(Schema.fromJsonString(ManualSnapshot));
    const decodeManualResult = Schema.decodeUnknownEffect(
      Schema.fromJsonString(
        Schema.Struct({ task: ScheduledTask, manualReceipt: McpManualRunReceipt }),
      ),
    );
    const manualUnavailable = (_cause: unknown) =>
      new OrchestratorMcpFailure({
        code: "orchestration_error",
        message: "The manual request receipt is unavailable; no new action was authorized.",
      });
    const manualUnknown = (request: RequestIdentity) =>
      new OrchestratorMcpFailure({
        code: "outcome_unknown",
        requestId: request.clientRequestId,
        lastConfirmedStage: "dispatch_started",
        message:
          "This manual run may have started. Inspect its native destination before choosing a new request key.",
      });
    interface ManualRow {
      request_id: string;
      generation: string;
      input_digest: string;
      task_id: string;
      incarnation: string;
      snapshot_json: string;
      started_at: string;
      stage: string;
      reference_json: string | null;
      result_json: string | null;
    }
    const readManual = (request: RequestIdentity) =>
      sql<ManualRow>`SELECT * FROM scheduled_manual_requests WHERE request_id=${request.id}`.pipe(
        Effect.mapError(manualUnavailable),
      );
    const manualFailed = (message: string) =>
      new OrchestratorMcpFailure({
        code: "orchestration_error",
        message: `The manual run was refused before it started: ${message}`,
      });
    const failManual = (
      request: RequestIdentity,
      taskId: ScheduledTaskId,
      incarnation: string,
      message: string,
      fromStage: "prepared" | "dispatching",
    ) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const claimed =
              yield* sql`UPDATE scheduled_manual_requests SET stage='failed',result_json=${message} WHERE request_id=${request.id} AND stage=${fromStage} AND result_json IS NULL RETURNING request_id`;
            if (claimed.length === 0) return;
            const active = (yield* sql<{
              active_request_id: string | null;
            }>`SELECT active_request_id FROM scheduled_task_request_incarnations WHERE task_id=${taskId} AND incarnation=${incarnation}`)[0];
            if (active?.active_request_id !== request.id) return;
            const completedAt = yield* localNow;
            const current = yield* loadTask(taskId);
            yield* sql`UPDATE scheduled_tasks SET run_count=run_count+1,last_run_status='failed',last_run_error=${message},updated_at=${iso(completedAt)},next_run_at=${nextRunAt(current, completedAt)} WHERE task_id=${taskId}`;
            yield* sql`UPDATE scheduled_task_request_incarnations SET active_request_id=NULL WHERE task_id=${taskId} AND active_request_id=${request.id}`;
          }),
        )
        .pipe(Effect.mapError(manualUnavailable), Effect.andThen(notifyChanged));
    const validateManualRun = (task: ScheduledTask) =>
      Effect.gen(function* () {
        if (!task.enabled && Option.isSome(yield* automation.current("schedule", task.id)))
          return yield* taskError("This schedule is paused. Resume it before running it now.", {
            taskId: task.id,
          });
      });

    const runManualNow = (input: ScheduledTaskRunNowInput, options: ManualOptions) =>
      Effect.gen(function* () {
        const request = options.request;
        if (Option.isNone(nativeReceipts)) return yield* manualUnavailable(undefined);
        const recheck = options.recheck ?? Effect.void;
        const localFence = yield* MutationReceipts.captureResourceFence(input.id).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        yield* recheck;
        // The owner freezes this exact intent and admission before any dispatch.
        const admission = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* localFence;
              const old = (yield* readManual(request))[0];
              const current = yield* loadTask(input.id);
              yield* sql`INSERT INTO scheduled_task_request_incarnations (task_id,incarnation)
          SELECT task_id,lower(hex(randomblob(16))) FROM scheduled_tasks WHERE task_id=${input.id} ON CONFLICT DO NOTHING`;
              const incarnation = (yield* sql<{
                incarnation: string;
                active_request_id: string | null;
              }>`SELECT incarnation,active_request_id FROM scheduled_task_request_incarnations WHERE task_id=${input.id}`)[0];
              if (incarnation === undefined) return yield* manualUnavailable(undefined);
              if (old !== undefined) {
                if (
                  old.generation !== request.generation ||
                  old.incarnation !== incarnation.incarnation
                )
                  return yield* new OrchestratorMcpFailure({
                    code: "capability_denied",
                    message:
                      "This manual request belongs to its original grant and schedule incarnation.",
                  });
                if (old.input_digest !== request.inputDigest || old.task_id !== input.id)
                  return yield* new OrchestratorMcpFailure({
                    code: "invalid_request",
                    message: "The manual request key was already bound to different input.",
                  });
                return { row: old, decision: undefined };
              }
              if (current.schedule.type === "webhook")
                return yield* taskError("Webhook tasks run when their URL receives a request.", {
                  taskId: input.id,
                });
              if (
                incarnation.active_request_id !== null ||
                (yield* Ref.get(activeRuns)).has(input.id)
              )
                return yield* taskError("Schedule task is already running.", { taskId: input.id });
              yield* validateManualRun(current).pipe(
                Effect.mapError((error) => manualFailed(error.message)),
              );
              yield* validateBinding(current);
              const startedAt = yield* localNow;
              const bareAuthority =
                current.bare === undefined
                  ? undefined
                  : yield* readScheduledBare(current.id).pipe(
                      Effect.provideService(SqlClient.SqlClient, sql),
                      Effect.mapError(manualUnavailable),
                    );
              const acceptedAutomation = yield* automation.current("schedule", current.id);
              const snapshot = {
                task: current,
                ...(Option.isSome(acceptedAutomation)
                  ? { automationCertificateId: acceptedAutomation.value.id }
                  : {}),
                ...(bareAuthority === undefined ? {} : { bareReadGrant: bareAuthority.readGrant }),
              };
              const decision = yield* dispatchPolicy
                .decide({
                  task: current,
                  trigger: "manual",
                  startedAt,
                  manualRequestId: request.id,
                })
                .pipe(Effect.mapError((error) => taskError(error.message, { taskId: current.id })));
              if (decision._tag === "fork" && decision.manualReceipt?.requestId !== request.id)
                return yield* new OrchestratorMcpFailure({
                  code: "invalid_request",
                  message:
                    "This existing scheduled action has no verified keyed manual admission. Use its existing recovery operation.",
                });
              const stage =
                decision._tag === "skip"
                  ? "skipped"
                  : decision._tag === "fork"
                    ? "admitted"
                    : "prepared";
              const encodedSnapshot = yield* encodeManualSnapshot(snapshot).pipe(
                Effect.mapError(manualUnavailable),
              );
              const reference =
                decision._tag === "fork"
                  ? yield* encodeManualReference(decision.manualReceipt!).pipe(
                      Effect.mapError(manualUnavailable),
                    )
                  : null;
              yield* sql`INSERT INTO scheduled_manual_requests VALUES (${request.id},${request.generation},${request.inputDigest},${input.id},${incarnation.incarnation},${encodedSnapshot},${iso(startedAt)},${stage},${reference},NULL)`;
              if (decision._tag !== "skip") {
                yield* markRunning(input.id, iso(startedAt));
                yield* sql`UPDATE scheduled_task_request_incarnations SET active_request_id=${request.id} WHERE task_id=${input.id}`;
              }
              return { row: (yield* readManual(request))[0]!, decision };
            }),
          )
          .pipe(
            Effect.mapError((error) =>
              error._tag === "OrchestratorMcpFailure" || error._tag === "ScheduledTaskError"
                ? error
                : manualUnavailable(error),
            ),
          );
        const { row, decision } = admission;
        const snapshot = yield* decodeManualSnapshot(row.snapshot_json).pipe(
          Effect.mapError(manualUnavailable),
        );
        // A typed refusal before this key claims dispatch never reached a
        // destination, so it is this key's outcome and releases the schedule.
        const refuseBeforeDispatch = (refusal: OrchestratorMcpFailure) =>
          failManual(request, input.id, row.incarnation, refusal.message, "prepared").pipe(
            Effect.andThen(Effect.fail(refusal)),
          );
        yield* row.stage === "prepared" && row.result_json === null
          ? recheck.pipe(Effect.catch(refuseBeforeDispatch))
          : recheck;
        const commandId = CommandId.make(`scheduled-task:manual:${request.id}`);
        const messageId = MessageId.make(`scheduled-task-message:manual:${request.id}`);
        const threadId = ThreadId.make(
          snapshot.task.threadId ?? `scheduled-manual-thread:${request.id}`,
        );
        const readAccepted = Effect.gen(function* () {
          const receipt = yield* nativeReceipts.value
            .getByCommandId(
              snapshot.task.threadId === null
                ? CommandId.make(`${commandId}:initial-message`)
                : commandId,
            )
            .pipe(Effect.mapError(manualUnavailable));
          if (Option.isNone(receipt)) return false;
          if (
            receipt.value.status !== "accepted" ||
            receipt.value.threadId !== threadId ||
            receipt.value.commandType !== "message.dispatch"
          )
            return yield* manualUnknown(request);
          return true;
        });
        if (row.stage === "failed") return yield* manualFailed(row.result_json ?? "");
        const native = row.stage !== "admitted" && row.stage !== "skipped";
        let accepted = native ? yield* readAccepted : false;
        if (row.result_json !== null) {
          if (native && !accepted) return yield* manualUnknown(request);
          return yield* decodeManualResult(row.result_json).pipe(
            Effect.mapError(manualUnavailable),
          );
        }
        if (native && !accepted) {
          yield* validateBinding(snapshot.task);
          // A launch can allocate a workspace before its first native receipt.
          // Missing evidence in that window must never allocate a second workspace.
          if (row.stage === "dispatching" && snapshot.task.threadId === null) {
            const create = yield* nativeReceipts.value
              .getByCommandId(commandId)
              .pipe(Effect.mapError(manualUnavailable));
            if (
              Option.isNone(create) ||
              create.value.status !== "accepted" ||
              create.value.threadId !== threadId ||
              create.value.commandType !== "thread.create"
            )
              return yield* manualUnknown(request);
          }
          // The final live check precedes the claim: once dispatching, a same-key
          // retry may already be sending, so a refusal is no longer this key's outcome.
          yield* row.stage === "prepared"
            ? recheck.pipe(Effect.catch(refuseBeforeDispatch))
            : recheck;
          const claimed =
            yield* sql`UPDATE scheduled_manual_requests SET stage='dispatching' WHERE request_id=${request.id} AND stage='prepared' RETURNING request_id`.pipe(
              Effect.mapError(manualUnavailable),
            );
          if (row.stage === "prepared" && claimed.length === 0)
            return yield* manualUnknown(request);
          const dispatched = yield* Effect.exit(
            dispatchTask(
              snapshot.task,
              commandId,
              messageId,
              snapshot.task.prompt,
              { _tag: "upstream" },
              threadId,
              snapshot.bareReadGrant,
              { certificateId: snapshot.automationCertificateId ?? null },
            ),
          );
          const failure: Cause.Cause<unknown> | undefined =
            dispatched._tag === "Failure"
              ? dispatched.cause
              : dispatched.value._tag === "Failure"
                ? dispatched.value.cause
                : undefined;
          accepted = yield* readAccepted;
          if (!accepted) {
            // A typed refusal with no native receipt never admitted anything:
            // record it as this key's outcome so the schedule is not left running.
            // Interruptions and defects stay unknown.
            const refused =
              failure !== undefined &&
              !Cause.hasInterrupts(failure) &&
              !Cause.hasDies(failure) &&
              (snapshot.task.threadId !== null ||
                Option.isNone(
                  yield* nativeReceipts.value
                    .getByCommandId(commandId)
                    .pipe(Effect.mapError(manualUnavailable)),
                ));
            if (!refused) return yield* manualUnknown(request);
            const message = errorMessage(Cause.squash(failure));
            yield* failManual(request, input.id, row.incarnation, message, "dispatching");
            return yield* manualFailed(message);
          }
        } else if (decision?._tag === "fork") {
          // Exact policy admission is durable. A retry observes it without
          // repeating a possibly external command; the policy owns its recovery.
          yield* decision.dispatch.pipe(Effect.catch(() => Effect.void));
        }
        const receipt = {
          requestId: request.clientRequestId,
          operationId: request.id,
          stage: native
            ? ("native-accepted" as const)
            : row.stage === "skipped"
              ? ("skipped" as const)
              : ("admitted" as const),
        };
        const result = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const fresh = (yield* readManual(request))[0]!;
              if (fresh.result_json !== null)
                return yield* decodeManualResult(fresh.result_json).pipe(
                  Effect.mapError(manualUnavailable),
                );
              const completedAt = yield* localNow;
              const current = yield* loadTask(input.id);
              const incarnation = (yield* sql<{
                incarnation: string;
                active_request_id: string | null;
              }>`SELECT incarnation,active_request_id FROM scheduled_task_request_incarnations WHERE task_id=${input.id}`)[0];
              if (incarnation?.incarnation !== row.incarnation)
                return yield* new OrchestratorMcpFailure({
                  code: "capability_denied",
                  message: "The original scheduled task was replaced.",
                });
              if (row.stage !== "skipped") {
                if (incarnation.active_request_id === request.id) {
                  yield* sql`UPDATE scheduled_tasks SET run_count=run_count+1,last_run_status='succeeded',last_run_error=NULL,updated_at=${iso(completedAt)},next_run_at=${nextRunAt(current, completedAt)} WHERE task_id=${input.id}`;
                  yield* sql`UPDATE scheduled_task_request_incarnations SET active_request_id=NULL WHERE task_id=${input.id} AND active_request_id=${request.id}`;
                } else
                  yield* sql`UPDATE scheduled_tasks SET run_count=run_count+1 WHERE task_id=${input.id}`;
              }
              const result = {
                task: {
                  ...snapshot.task,
                  lastRunAt: row.started_at,
                  ...(row.stage === "skipped"
                    ? {}
                    : {
                        lastRunStatus: "succeeded" as const,
                        lastRunError: null,
                        runCount: snapshot.task.runCount + 1,
                        updatedAt: iso(completedAt),
                        nextRunAt: nextRunAt(snapshot.task, completedAt),
                      }),
                },
                manualReceipt: receipt,
              };
              const encodedResult = yield* encodeManualResult(result).pipe(
                Effect.mapError(manualUnavailable),
              );
              yield* sql`UPDATE scheduled_manual_requests SET result_json=${encodedResult} WHERE request_id=${request.id}`;
              return result;
            }),
          )
          .pipe(
            Effect.mapError((error) =>
              error._tag === "OrchestratorMcpFailure" || error._tag === "ScheduledTaskError"
                ? error
                : manualUnavailable(error),
            ),
          );
        yield* notifyChanged;
        return result;
      });

    const runLocalNow = (input: ScheduledTaskRunNowInput) =>
      Effect.gen(function* () {
        const task = yield* loadTask(input.id);
        if (task.schedule.type === "webhook") {
          return yield* taskError("Webhook tasks run when their URL receives a request.", {
            taskId: input.id,
          });
        }
        yield* validateManualRun(task).pipe(
          Effect.mapError((cause) =>
            cause._tag === "ScheduledTaskError"
              ? cause
              : taskError("Could not validate schedule task.", { taskId: input.id, cause }),
          ),
        );
        const next = yield* runTask(task, "manual").pipe(
          Effect.mapError((cause) =>
            taskError("Could not run schedule task.", { taskId: input.id, cause }),
          ),
        );
        return { task: next };
      });

    function runNow(
      input: ScheduledTaskRunNowInput,
    ): Effect.Effect<ScheduledTaskRunNowResult, ScheduledTaskError>;
    function runNow(
      input: ScheduledTaskRunNowInput,
      options: ManualOptions,
    ): Effect.Effect<ScheduledTaskRunNowResult, ScheduledTaskError | OrchestratorMcpFailure>;
    function runNow(input: ScheduledTaskRunNowInput, options?: ManualOptions) {
      return options === undefined ? runLocalNow(input) : runManualNow(input, options);
    }

    const rotateWebhookToken: ScheduledTaskService["Service"]["rotateWebhookToken"] = (input) =>
      Effect.gen(function* () {
        const task = yield* loadTask(input.id);
        if (task.schedule.type !== "webhook") {
          return yield* taskError("Only webhook tasks have a URL token.", { taskId: input.id });
        }
        const token = yield* newWebhookToken;
        const now = yield* localNow;
        // Matching created_at keeps a rotation from landing on a task deleted
        // and recreated under the same id since it was loaded.
        const updated = yield* sql<{ task_id: string }>`
          UPDATE scheduled_tasks
          SET webhook_token = ${token}, updated_at = ${iso(now)}
          WHERE task_id = ${input.id} AND created_at = ${task.createdAt}
          RETURNING task_id
        `.pipe(
          Effect.mapError((cause) =>
            taskError("Could not rotate webhook token.", { taskId: input.id, cause }),
          ),
        );
        if (updated.length === 0) {
          return yield* taskError("Schedule task was deleted or replaced.", { taskId: input.id });
        }
        yield* notifyChanged;
        return { task: yield* loadTask(input.id) };
      });

    const deliveryHeaders = (row: WebhookDeliveryRow): Readonly<Record<string, string>> =>
      Option.getOrElse(decodeHeadersJson(row.headers_json), () => ({}));
    const decodeDeliverySummary = (row: WebhookDeliveryRow) => ({
      id: ScheduledTaskWebhookDeliveryId.make(row.delivery_id),
      taskId: ScheduledTaskId.make(row.task_id),
      receivedAt: row.received_at,
      method: row.method,
      contentType: deliveryHeaders(row)["content-type"] ?? null,
      bodyBytes: row.body_bytes,
      outcome: row.outcome as ScheduledTaskWebhookDeliveryOutcome,
      signatureVerified: row.signature_verified === 1,
      missingFields: Option.getOrElse(decodeMissingFieldsJson(row.missing_fields_json), () => []),
      error: row.error,
    });

    const listWebhookDeliveries: ScheduledTaskService["Service"]["listWebhookDeliveries"] = (
      input,
    ) =>
      sql<WebhookDeliveryRow>`
        SELECT * FROM scheduled_task_webhook_deliveries
        WHERE task_id = ${input.id}
        ORDER BY received_at DESC, rowid DESC
      `.pipe(
        Effect.tap((rows) => {
          const deferredCount = rows.filter((row) =>
            ["duplicate", "coalesced", "filtered"].includes(row.outcome),
          ).length;
          return deferredCount === 0
            ? Effect.void
            : Effect.logWarning("Newer delivery history retained until roll-forward", {
                taskId: input.id,
                deferredCount,
              });
        }),
        Effect.map((rows) => ({
          deliveries: rows
            .filter((row) => !["duplicate", "coalesced", "filtered"].includes(row.outcome))
            .map(decodeDeliverySummary),
        })),
        Effect.mapError((cause) =>
          taskError("Could not list webhook deliveries.", { taskId: input.id, cause }),
        ),
      );

    const getWebhookDelivery: ScheduledTaskService["Service"]["getWebhookDelivery"] = (input) =>
      Effect.gen(function* () {
        const rows = yield* sql<WebhookDeliveryRow>`
          SELECT * FROM scheduled_task_webhook_deliveries
          WHERE task_id = ${input.id} AND delivery_id = ${input.deliveryId}
        `.pipe(
          Effect.mapError((cause) =>
            taskError("Could not load webhook delivery.", { taskId: input.id, cause }),
          ),
        );
        const row = rows[0];
        if (row === undefined) {
          return yield* taskError("Webhook delivery not found.", { taskId: input.id });
        }
        if (["duplicate", "coalesced", "filtered"].includes(row.outcome))
          return yield* taskError(
            "Newer delivery history is retained and deferred until roll-forward.",
            { taskId: input.id },
          );
        return {
          delivery: {
            ...decodeDeliverySummary(row),
            query: row.query,
            headers: deliveryHeaders(row),
            body: row.body,
            bodyTruncated: row.body_truncated === 1,
            renderedPrompt: row.rendered_prompt,
          },
        };
      });

    const recordDelivery = (input: {
      readonly id: string;
      readonly taskId: ScheduledTaskId;
      readonly receivedAt: string;
      readonly request: WebhookTriggerRequest;
      readonly outcome: ScheduledTaskWebhookDeliveryOutcome;
      readonly signatureVerified: boolean;
      readonly missing: ReadonlyArray<string>;
      readonly renderedPrompt: string | null;
      readonly error?: string;
    }) => {
      const truncated = input.request.body.byteLength > WEBHOOK_DELIVERY_LOG_BODY_LIMIT;
      const loggedBody = truncated
        ? new TextDecoder().decode(input.request.body.subarray(0, WEBHOOK_DELIVERY_LOG_BODY_LIMIT))
        : input.request.bodyText;
      return sql
        .withTransaction(
          Effect.gen(function* () {
            // Conditional on the task existing, so a delivery racing a delete
            // cannot leave rows that a recreated task with the same id would show.
            yield* sql`
              INSERT INTO scheduled_task_webhook_deliveries (
                delivery_id, task_id, received_at, method, query, headers_json, body,
                body_bytes, body_truncated, outcome, signature_verified,
                missing_fields_json, rendered_prompt, error
              )
              SELECT
                ${input.id}, ${input.taskId}, ${input.receivedAt}, ${input.request.method},
                ${redactQuery(input.request.query)},
                ${encodeHeadersJson(redactHeaders(input.request.headers))},
                ${loggedBody},
                ${input.request.body.byteLength}, ${truncated ? 1 : 0}, ${input.outcome},
                ${input.signatureVerified ? 1 : 0}, ${encodeMissingFieldsJson(input.missing)},
                ${input.renderedPrompt?.slice(0, WEBHOOK_DELIVERY_LOG_PROMPT_LIMIT) ?? null},
                ${input.error ?? null}
              WHERE EXISTS (SELECT 1 FROM scheduled_tasks WHERE task_id = ${input.taskId})
              -- A held request retried after a rate limit reuses its relay
              -- delivery id; the newer attempt replaces the logged one.
              ON CONFLICT (delivery_id) DO UPDATE SET
                outcome = excluded.outcome,
                signature_verified = excluded.signature_verified,
                missing_fields_json = excluded.missing_fields_json,
                rendered_prompt = excluded.rendered_prompt,
                error = excluded.error
            `;
          }),
        )
        .pipe(
          Effect.mapError((cause) =>
            taskError("Could not record webhook delivery.", { taskId: input.taskId, cause }),
          ),
        );
    };

    const markDeliveryFailed = (deliveryId: string, message: string) =>
      sql`
        UPDATE scheduled_task_webhook_deliveries
        SET outcome = 'dispatch_failed', error = ${message}
        WHERE delivery_id = ${deliveryId}
      `.pipe(Effect.ignore);

    /**
     * Sliding one-minute window counting every request with a valid token,
     * including ones the signature check later rejects.
     */
    const takeRateSlot = (id: ScheduledTaskId, nowMs: number) =>
      Ref.modify(
        webhookRateWindows,
        (
          windows,
        ): readonly [
          "allowed" | "first_rejected" | "rejected",
          ReadonlyMap<ScheduledTaskId, RateWindow>,
        ] => {
          const current = windows.get(id);
          const recent = (current?.accepted ?? []).filter((at) => nowMs - at < 60_000);
          if (recent.length >= WEBHOOK_RATE_LIMIT_PER_MINUTE) {
            const first = current?.rejectedLogged !== true;
            return [
              first ? "first_rejected" : "rejected",
              new Map(windows).set(id, { accepted: recent, rejectedLogged: true }),
            ];
          }
          return [
            "allowed",
            new Map(windows).set(id, { accepted: [...recent, nowMs], rejectedLogged: false }),
          ];
        },
      );

    const webhookPermit = (id: ScheduledTaskId) =>
      Effect.gen(function* () {
        const existing = (yield* Ref.get(webhookPermits)).get(id);
        if (existing !== undefined) return existing;
        const created = yield* Semaphore.make(1);
        return yield* Ref.modify(webhookPermits, (permits) => {
          const raced = permits.get(id);
          return raced === undefined
            ? [created, new Map(permits).set(id, created)]
            : [raced, permits];
        });
      });

    const serviceScope = yield* Effect.scope;

    const observeDelivery = (
      request: WebhookTriggerRequest,
      outcome: WebhookDeliveryOutcome,
      receivedAt: DateTime.DateTime | undefined,
      now: DateTime.DateTime | undefined,
    ) =>
      Effect.gen(function* () {
        const source = request.relayDeliveryId === undefined ? "direct" : "relay";
        yield* Effect.annotateCurrentSpan({
          "scheduled_task.webhook.outcome": outcome,
          "scheduled_task.webhook.source": source,
        });
        yield* Metrics.increment(Metrics.webhookDeliveriesTotal, { outcome, source });
        if (request.receivedAt !== undefined && receivedAt !== undefined && now !== undefined) {
          const heldMs = Math.max(
            0,
            DateTime.toEpochMillis(now) - DateTime.toEpochMillis(receivedAt),
          );
          yield* Effect.annotateCurrentSpan({ "scheduled_task.webhook.held_ms": heldMs });
          yield* Metric.update(Metrics.webhookHeldDelay, Duration.millis(heldMs));
        }
      });

    const triggerWebhook: ScheduledTaskService["Service"]["triggerWebhook"] = (request) =>
      triggerWebhookUnobserved(request).pipe(
        Effect.tapError(() => observeDelivery(request, "error", undefined, undefined)),
        Metrics.withMetrics({ timer: Metrics.webhookDeliveryDuration }),
        Effect.withSpan("ScheduledTaskService.triggerWebhook", {
          attributes: {
            "scheduled_task.webhook.method": request.method,
            "scheduled_task.webhook.body_bytes": request.body.byteLength,
            "scheduled_task.webhook.source":
              request.relayDeliveryId === undefined ? "direct" : "relay",
          },
        }),
      );

    const triggerWebhookUnobserved = (request: WebhookTriggerRequest) =>
      Effect.gen(function* () {
        const taskId = decodeTaskId(request.hookId);
        const notFound = observeDelivery(request, "not_found", undefined, undefined).pipe(
          Effect.as({ _tag: "not_found" as const }),
        );
        if (Option.isNone(taskId)) return yield* notFound;
        const rows = yield* getRows(taskId.value).pipe(
          Effect.mapError((cause) =>
            taskError("Could not load schedule task.", { taskId: taskId.value, cause }),
          ),
        );
        const row = rows[0];
        // A wrong token is indistinguishable from an unknown hook, so the URL
        // does not reveal which hooks exist.
        if (
          row === undefined ||
          row.webhook_token === null ||
          !constantTimeEquals(request.token, row.webhook_token)
        ) {
          return yield* notFound;
        }
        const task = yield* decodeRow(row);
        yield* Effect.annotateCurrentSpan({ "scheduled_task.id": task.id });
        const schedule = task.schedule;
        if (schedule.type !== "webhook") return yield* notFound;

        const now = yield* localNow;
        // Anyone can reach the tunnel directly and set the relay's header, so
        // a receive time is never later than now: a future one would pin the
        // delivery in the log and slip past the task's max age.
        const receivedAt =
          request.receivedAt === undefined
            ? now
            : DateTime.min(
                Option.getOrElse(DateTime.make(request.receivedAt), () => now),
                now,
              );
        const observe = (outcome: WebhookDeliveryOutcome) =>
          observeDelivery(request, outcome, receivedAt, now);
        const deliveryId = ScheduledTaskWebhookDeliveryId.make(
          request.relayDeliveryId === undefined
            ? `delivery:${yield* crypto
                .randomUUIDv4()
                .pipe(
                  Effect.mapError((cause) =>
                    taskError("Could not generate delivery id.", { cause }),
                  ),
                )}`
            : `delivery:relay:${request.relayDeliveryId}`,
        );
        // Claimed in its own table, kept longer than the relay holds a request,
        // because the delivery log is trimmed. A rate-limited delivery stays
        // held on the relay, so it must give up its claim or the next pass
        // would treat it as already delivered.
        const releaseClaim = <A>(result: A) =>
          request.relayDeliveryId === undefined
            ? Effect.succeed(result)
            : sql`
                DELETE FROM scheduled_task_webhook_relay_deliveries
                WHERE relay_delivery_id = ${request.relayDeliveryId}
              `.pipe(Effect.ignore, Effect.as(result));
        // From the claim until the run is forked nothing may interrupt: a
        // request dropped in between (the relay hangs up after its timeout)
        // would leave a claimed delivery that never runs, or a queue slot that
        // is never released.
        return yield* Effect.uninterruptible(
          Effect.gen(function* () {
            const persistedSchedule = yield* decodeRawObject(row.schedule_json).pipe(
              Effect.mapError((cause) =>
                taskError("Could not read stored webhook settings.", { taskId: task.id, cause }),
              ),
            );
            const rawSchedule =
              persistedSchedule.type === "destination-automation-v1"
                ? persistedSchedule.schedule
                : persistedSchedule;
            if (
              typeof rawSchedule === "object" &&
              rawSchedule !== null &&
              ("dedupe" in rawSchedule || "filters" in rawSchedule)
            )
              return yield* taskError(
                "Newer webhook filtering/deduplication is deferred until roll-forward.",
                { taskId: task.id },
              );
            if (
              (yield* sql`SELECT 1 FROM scheduled_task_webhook_deliveries WHERE delivery_id = ${deliveryId} AND outcome <> 'rate_limited' LIMIT 1`.pipe(
                Effect.mapError((cause) =>
                  taskError("Could not check retained delivery.", { taskId: task.id, cause }),
                ),
              )).length > 0
            ) {
              yield* observe("duplicate");
              return { _tag: "accepted" as const, deliveryId, outcome: "duplicate" as const };
            }
            yield* checkDeferredTask(task.id);
            if (request.relayDeliveryId !== undefined) {
              const claimed = yield* sql<{ relay_delivery_id: string }>`
                INSERT INTO scheduled_task_webhook_relay_deliveries
                  (relay_delivery_id, task_id, seen_at)
                VALUES (${request.relayDeliveryId}, ${task.id}, ${iso(now)})
                ON CONFLICT (relay_delivery_id) DO NOTHING
                RETURNING relay_delivery_id
              `.pipe(
                Effect.mapError((cause) =>
                  taskError("Could not record webhook delivery.", { taskId: task.id, cause }),
                ),
              );
              if (claimed.length === 0) {
                yield* observe("duplicate");
                return { _tag: "accepted" as const, deliveryId, outcome: "duplicate" as const };
              }
              yield* sql`
                DELETE FROM scheduled_task_webhook_relay_deliveries
                WHERE seen_at < ${iso(DateTime.subtract(now, { hours: 48 }))}
              `.pipe(Effect.ignore);
            }
            const log = (
              outcome: ScheduledTaskWebhookDeliveryOutcome,
              details: {
                readonly signatureVerified?: boolean;
                readonly missing?: ReadonlyArray<string>;
                readonly renderedPrompt?: string;
                readonly error?: string;
              } = {},
            ) =>
              recordDelivery({
                id: deliveryId,
                taskId: task.id,
                receivedAt: iso(receivedAt),
                request,
                outcome,
                signatureVerified: details.signatureVerified ?? false,
                missing: details.missing ?? [],
                renderedPrompt: details.renderedPrompt ?? null,
                ...(details.error === undefined ? {} : { error: details.error }),
              });

            // Only the first rejected request in a window is logged, so a flood
            // cannot write rows or push the real deliveries out of the log.
            const slot = yield* takeRateSlot(task.id, DateTime.toEpochMillis(now));
            if (slot !== "allowed") {
              if (slot === "first_rejected") yield* log("rate_limited");
              yield* observe("rate_limited");
              return yield* releaseClaim({
                _tag: "rate_limited" as const,
                outcome: "rate_limited" as const,
              });
            }
            if (!task.enabled) {
              yield* log("disabled");
              yield* observe("disabled");
              return { _tag: "disabled" as const };
            }
            const signature = schedule.signature;
            if (signature !== null) {
              const verified =
                row.webhook_secret !== null &&
                verifyWebhookSignature({
                  signature,
                  secret: row.webhook_secret,
                  headers: request.headers,
                  body: request.body,
                });
              if (!verified) {
                yield* log("rejected_signature");
                yield* observe("rejected_signature");
                return { _tag: "rejected_signature" as const };
              }
            }
            const maxAgeMinutes = schedule.maxDeliveryAgeMinutes ?? null;
            if (
              maxAgeMinutes !== null &&
              DateTime.toEpochMillis(now) - DateTime.toEpochMillis(receivedAt) >
                maxAgeMinutes * 60_000
            ) {
              yield* log("expired", { signatureVerified: signature !== null });
              yield* observe("expired");
              return { _tag: "expired" as const };
            }

            const frameId = yield* crypto.randomBytes(6).pipe(
              Effect.map((bytes) => Buffer.from(bytes).toString("hex")),
              Effect.mapError((cause) => taskError("Could not generate frame id.", { cause })),
            );
            const rendered = renderWebhookPrompt(task.prompt, request, frameId);
            // Providers trim the prompt before checking, so whitespace around
            // it is free.
            if (rendered.prompt.trim().length > PROVIDER_SEND_TURN_MAX_INPUT_CHARS) {
              yield* log("dispatch_failed", {
                signatureVerified: signature !== null,
                missing: rendered.missing,
                renderedPrompt: rendered.prompt,
                error: "The filled-in prompt is too long.",
              });
              yield* observe("prompt_too_long");
              return { _tag: "accepted" as const, deliveryId, outcome: "prompt_too_long" as const };
            }
            // Bound the deliveries one task holds, so steady traffic to a stuck
            // task cannot pile up parked fibers.
            const queueKey = `${task.id}\u0000${task.createdAt}`;
            const queued = yield* Ref.modify(webhookQueued, (counts) => {
              const count = counts.get(queueKey) ?? 0;
              return count >= WEBHOOK_MAX_QUEUED_PER_TASK
                ? ([false, counts] as const)
                : ([true, new Map(counts).set(queueKey, count + 1)] as const);
            });
            if (!queued) {
              yield* observe("queue_full");
              return yield* releaseClaim({
                _tag: "rate_limited" as const,
                outcome: "queue_full" as const,
              });
            }
            const release = Ref.update(webhookQueued, (counts) => {
              const next = new Map(counts);
              const count = (next.get(queueKey) ?? 1) - 1;
              if (count <= 0) next.delete(queueKey);
              else next.set(queueKey, count);
              return next;
            });
            yield* log("accepted", {
              signatureVerified: signature !== null,
              missing: rendered.missing,
              renderedPrompt: rendered.prompt,
            }).pipe(Effect.onError(() => release));
            yield* observe("accepted");
            const permit = yield* webhookPermit(task.id);
            const runOutcome = (outcome: "started" | "skipped" | "failed") =>
              Effect.all([
                Effect.annotateCurrentSpan({ "scheduled_task.webhook.run_outcome": outcome }),
                Metrics.increment(Metrics.webhookRunsTotal, { outcome }),
              ]);
            yield* runTask(task, "webhook", { deliveryId, prompt: rendered.prompt }).pipe(
              Effect.flatMap((completed) =>
                completed.lastRunStatus === "failed"
                  ? runOutcome("failed").pipe(
                      Effect.andThen(markDeliveryFailed(deliveryId, "The run failed to start.")),
                    )
                  : runOutcome("started"),
              ),
              Effect.catchTags({
                WebhookDeliverySkipped: (skipped) =>
                  runOutcome("skipped").pipe(
                    Effect.andThen(markDeliveryFailed(deliveryId, skipped.reason)),
                  ),
              }),
              // The log is readable over RPC, so it gets a fixed reason; the
              // cause, which can carry request data, stays in the server log.
              Effect.catchCause((cause) =>
                Effect.logWarning("Webhook dispatch failed", { taskId: task.id, cause }).pipe(
                  Effect.andThen(runOutcome("failed")),
                  Effect.andThen(markDeliveryFailed(deliveryId, "The run failed to start.")),
                ),
              ),
              permit.withPermits(1),
              Effect.withSpan("ScheduledTaskService.runWebhookDelivery", {
                root: true,
                attributes: {
                  "scheduled_task.id": task.id,
                  "scheduled_task.webhook.delivery_id": deliveryId,
                },
              }),
              Effect.ensuring(release),
              Effect.forkIn(serviceScope),
            );
            return { _tag: "accepted" as const, deliveryId, outcome: "accepted" as const };
          }),
        );
      });

    return ScheduledTaskService.of({
      mutationReceipt: (request) =>
        MutationReceipts.confirmed(request).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
      list,
      subscribeList,
      upsert,
      setEnabled,
      delete: deleteTask,
      runNow,
      rotateWebhookToken,
      listWebhookDeliveries,
      getWebhookDelivery,
      triggerWebhook,
    });
  }),
).pipe(Layer.provide(Automation.layer));
