import {
  defaultInstanceIdForDriver,
  EventId,
  OrchestrationV2ProviderThreadJson,
  ProviderDriverKind,
  ProviderSessionId,
  type OrchestrationV2ProviderThread,
  type ProviderInstanceConfig,
  type ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { EventSinkV2 } from "../orchestration-v2/EventSink.ts";
import * as ServerSettings from "../serverSettings.ts";

export const openCode2SessionRebindId = "opencode2-session-rebind";

export class OpenCode2SessionRebindError extends Schema.TaggedError<OpenCode2SessionRebindError>()(
  "OpenCode2SessionRebindError",
  { reason: Schema.Literals(["settings", "providerThread"]) },
) {}

const decodeProviderThread = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ProviderThreadJson),
);

const OPENCODE = ProviderDriverKind.make("opencode");

/** A configured `serverUrl` keeps one shared server, so its threads keep sharing a session. */
const runsOwnProcesses = (
  instanceId: ProviderInstanceId,
  instance: ProviderInstanceConfig | undefined,
) => {
  if (instance === undefined) return instanceId === defaultInstanceIdForDriver(OPENCODE);
  if (instance.driver !== OPENCODE) return false;
  const config = instance.config;
  if (typeof config !== "object" || config === null || !("serverUrl" in config)) return true;
  return typeof config.serverUrl !== "string" || config.serverUrl.trim().length === 0;
};

/**
 * Local OpenCode 2 threads once shared their instance's session; each now runs its own process.
 * Every app thread stored on that shared id gets its own, keeping its native OpenCode session.
 */
export function openCode2SessionRebinds(
  providerThreads: ReadonlyArray<{
    readonly threadId: ThreadId;
    readonly providerThread: OrchestrationV2ProviderThread;
  }>,
  instances: Readonly<Record<string, ProviderInstanceConfig>>,
): ReadonlyArray<{
  readonly threadId: ThreadId;
  readonly providerThread: OrchestrationV2ProviderThread;
}> {
  return providerThreads.flatMap(({ threadId, providerThread }) => {
    const instanceId: ProviderInstanceId = providerThread.providerInstanceId;
    if (
      providerThread.providerSessionId !==
        IdAllocator.derive.providerSession({ providerInstanceId: instanceId }) ||
      !runsOwnProcesses(instanceId, instances[instanceId])
    )
      return [];
    return [
      {
        threadId,
        providerThread: {
          ...providerThread,
          providerSessionId: ProviderSessionId.make(
            `provider-session:provider-instance:${encodeURIComponent(instanceId)}:thread:${encodeURIComponent(threadId)}:own-process`,
          ),
        },
      },
    ];
  });
}

/** Runs before recovery reopens any session; without readable settings it changes nothing. */
export const rebindOpenCode2SharedSessions = Effect.fn("fork.rebindOpenCode2SharedSessions")(
  function* () {
    const settings = yield* Effect.serviceOption(ServerSettings.ServerSettingsService);
    if (Option.isNone(settings)) return;
    const current = yield* settings.value.getSettings.pipe(
      Effect.mapError(() => new OpenCode2SessionRebindError({ reason: "settings" })),
    );
    const sql = yield* SqlClient.SqlClient;
    const sink = yield* EventSinkV2;
    const rows = yield* sql<{ thread_id: string; payload_json: string }>`
      SELECT thread_id, payload_json FROM orchestration_v2_projection_provider_threads
      WHERE provider_session_id LIKE '%:shared'
    `;
    const providerThreads = yield* Effect.forEach(rows, (row) =>
      decodeProviderThread(row.payload_json).pipe(
        Effect.map((providerThread) => ({
          threadId: ThreadId.make(row.thread_id),
          providerThread,
        })),
        Effect.mapError(() => new OpenCode2SessionRebindError({ reason: "providerThread" })),
      ),
    );
    for (const { threadId, providerThread } of openCode2SessionRebinds(
      providerThreads,
      current.providerInstances,
    )) {
      yield* sink.write({
        events: [
          {
            id: EventId.make(`migration:fork:opencode2-session-rebind:${providerThread.id}`),
            type: "provider-thread.updated",
            threadId,
            driver: providerThread.driver,
            providerInstanceId: providerThread.providerInstanceId,
            occurredAt: providerThread.updatedAt,
            payload: providerThread,
          },
        ],
      });
    }
  },
);
