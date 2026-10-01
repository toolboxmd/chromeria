import {
  EventId,
  type OrchestrationCommand,
  type OrchestrationEvent,
  type OrchestrationReadModel,
  type OrchestrationThreadActivity,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { OrchestrationCommandInvariantError } from "./Errors.ts";
import {
  isDescendantThreadId,
  parentThreadIdOf,
} from "../mcp/toolkits/threads/subagentThreadId.ts";

export const RETIREMENT_KIND = "thread.retirement";
export const RETIRE_SUBTREE_KIND = "thread.subtree-retire-requested";
const Retirement = Schema.Struct({
  threadId: Schema.String,
  retired: Schema.Boolean,
  cutoffSequence: Schema.Number,
  pendingStop: Schema.Boolean,
  stopAckCommandId: Schema.String,
  activeTurnId: Schema.NullOr(Schema.String),
  hadActiveRun: Schema.Boolean,
});
export type ThreadRetirement = typeof Retirement.Type;
const decodeRetirement = Schema.decodeUnknownOption(Retirement);

export function retirementFrom(activity: OrchestrationThreadActivity) {
  return activity.kind === RETIREMENT_KIND ? decodeRetirement(activity.payload) : undefined;
}

/** Nearest explicit recovery wins; existing descendants retain their own retirement. */
export function retirementOf(states: ReadonlyMap<string, ThreadRetirement>, threadId: string) {
  for (let id: string | null = threadId; id !== null; id = parentThreadIdOf(id)) {
    const state = states.get(id);
    if (state) return state;
  }
  return undefined;
}

export function blocksQueuedStart(state: ThreadRetirement | undefined, sequence: number) {
  return state !== undefined && (state.retired || sequence <= state.cutoffSequence);
}

/** Automatic toolkit turns and server continuations never count as explicit recovery. */
function isAutomaticTurn(commandId: string) {
  return commandId.startsWith("server:") && !commandId.startsWith("server:mcp-threads-message:");
}

const marker = (
  threadId: ThreadId,
  retired: boolean,
  cutoffSequence: number,
  createdAt: string,
  pendingStop = false,
  stopAckCommandId = "",
  activeTurnId: string | null = null,
  hadActiveRun = false,
) => ({
  id: EventId.make(`thread-retirement:${threadId}`),
  kind: RETIREMENT_KIND,
  tone: "info" as const,
  summary: retired ? "Thread retired" : "Thread explicitly resumed",
  payload: {
    threadId,
    retired,
    cutoffSequence,
    pendingStop,
    stopAckCommandId,
    activeTurnId,
    hadActiveRun,
  },
  turnId: null,
  createdAt,
});

/** Runs inside the engine's serialized command decision, before persistence. */
export const retirementCommands = (
  command: OrchestrationCommand,
  readModel: OrchestrationReadModel,
  states: ReadonlyMap<string, ThreadRetirement>,
) =>
  Effect.gen(function* () {
    for (const prefix of ["server:mcp-threads-message:", "server:mcp-threads-create:"]) {
      if (!command.commandId.startsWith(prefix)) continue;
      const [callerId, generation] = command.commandId.slice(prefix.length).split(":");
      const callerState = retirementOf(states, callerId!);
      if (callerState?.retired || Number(generation) !== (callerState?.cutoffSequence ?? 0))
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Calling thread ${callerId} is retired.`,
        });
    }
    if (
      command.type === "thread.activity.append" &&
      command.activity.kind === RETIRE_SUBTREE_KIND
    ) {
      const threads = readModel.threads.filter(
        (thread) =>
          thread.id === command.threadId || isDescendantThreadId(thread.id, command.threadId),
      );
      const commands: OrchestrationCommand[] = [];
      for (const thread of threads) {
        commands.push({
          type: "thread.activity.append",
          commandId: command.commandId,
          threadId: thread.id,
          activity: marker(
            thread.id,
            true,
            readModel.snapshotSequence,
            command.createdAt,
            true,
            `server:retirement-stopped:${command.commandId}:${thread.id}`,
            thread.session?.activeTurnId ?? null,
            thread.session?.status === "running" || thread.session?.status === "starting",
          ),
          createdAt: command.createdAt,
        });
        // Stop the provider session too: a starting turn need not have a turn id yet.
        commands.push({
          type: "thread.session.stop",
          commandId: command.commandId,
          threadId: thread.id,
          createdAt: command.createdAt,
        });
      }
      // Keep the caller's aggregate last, so its receipt belongs to the original target.
      return [...commands, command];
    }
    if (command.type === "thread.create") {
      const parentId = parentThreadIdOf(command.threadId);
      if (parentId !== null && retirementOf(states, parentId)?.retired) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Parent thread ${parentId} is retired; send it an explicit message before spawning.`,
        });
      }
    }
    if (!("threadId" in command)) return [command];
    const state = retirementOf(states, command.threadId);
    if (command.type === "thread.turn.start") {
      const reportPrefix = "server:mcp-threads-report:";
      if (command.commandId.startsWith(reportPrefix)) {
        const sourceId = command.commandId.slice(reportPrefix.length).split(":")[0]!;
        if (retirementOf(states, sourceId)?.retired)
          return yield* new OrchestrationCommandInvariantError({
            commandType: command.type,
            detail: `Reporting thread ${sourceId} is retired.`,
          });
      }

      if (!state?.retired) return [command];
      if (state.pendingStop && !isAutomaticTurn(command.commandId))
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Thread ${command.threadId} is still stopping; wait for provider stop acknowledgement before recovery.`,
        });
      if (isAutomaticTurn(command.commandId)) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Thread ${command.threadId} is retired; automatic turns are disabled.`,
        });
      }
      return [
        {
          type: "thread.activity.append" as const,
          commandId: command.commandId,
          threadId: command.threadId,
          activity: marker(command.threadId, false, state.cutoffSequence, command.createdAt),
          createdAt: command.createdAt,
        },
        command,
      ];
    }
    if (!state?.retired) return [command];
    // Late provider lifecycle writes cannot change a retirement back to idle/error/running.
    if (command.type === "thread.session.set") {
      if (command.commandId === state.stopAckCommandId) {
        return [
          {
            type: "thread.activity.append" as const,
            commandId: command.commandId,
            threadId: command.threadId,
            activity: marker(
              command.threadId,
              true,
              state.cutoffSequence,
              command.createdAt,
              false,
              state.stopAckCommandId,
              state.activeTurnId,
              state.hadActiveRun,
            ),
            createdAt: command.createdAt,
          },
          {
            ...command,
            session: {
              ...command.session,
              status: "interrupted" as const,
              activeTurnId: null,
              lastError: null,
            },
          },
        ];
      }
      if (state.pendingStop) {
        const previous = readModel.threads.find(
          (thread) => thread.id === command.threadId,
        )?.session;
        // Provider lifecycle noise is not proof that stopSession completed.
        return previous ? [{ ...command, session: previous }] : [command];
      }
      return [
        {
          ...command,
          session: {
            ...command.session,
            status: "interrupted" as const,
            activeTurnId: null,
            lastError: null,
          },
        },
      ];
    }
    return [command];
  });

export function rememberRetirement(
  states: Map<string, ThreadRetirement>,
  event: OrchestrationEvent,
) {
  if (event.type !== "thread.activity-appended") return;
  const decoded = retirementFrom(event.payload.activity);
  if (decoded && decoded._tag === "Some") states.set(decoded.value.threadId, decoded.value);
}
