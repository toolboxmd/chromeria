import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
} from "@t3tools/client-runtime/state/runtime";
import type { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { connectionAtomRuntime } from "../connection/runtime";

/**
 * Scheduled tasks of one environment. Scheduler state is not part of the shell stream, so the
 * list rereads on a timer while Settings shows it, and after every change made here.
 */
export const scheduledTaskList = createEnvironmentRpcQueryAtomFamily(connectionAtomRuntime, {
  label: "environment-data:scheduler:list",
  tag: "scheduler.list",
  staleTimeMs: 5_000,
  refreshIntervalMs: 15_000,
});

const refreshList = (
  target: { readonly environmentId: EnvironmentId },
  registry: { refresh: (atom: ReturnType<typeof scheduledTaskList>) => void },
) =>
  Effect.sync(() =>
    registry.refresh(scheduledTaskList({ environmentId: target.environmentId, input: {} })),
  );

export const scheduledTaskCreate = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "environment-data:scheduler:create",
  tag: "scheduler.create",
  onSuccess: refreshList,
});

export const scheduledTaskEdit = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "environment-data:scheduler:edit",
  tag: "scheduler.edit",
  onSuccess: refreshList,
});

export const scheduledTaskPause = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "environment-data:scheduler:pause",
  tag: "scheduler.pause",
  onSuccess: refreshList,
});

export const scheduledTaskDelete = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "environment-data:scheduler:delete",
  tag: "scheduler.delete",
  onSuccess: refreshList,
});

export const scheduledTaskRunNow = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "environment-data:scheduler:run-now",
  tag: "scheduler.runNow",
  onSuccess: refreshList,
});

/** One page of a task's check versions, newest first, read on demand. */
export const scheduledTaskCheckHistory = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "environment-data:scheduler:check-history",
  tag: "scheduler.checkHistory",
});

/**
 * The compact list the failure notifications watch in every environment: each task's active
 * check and newest run, without command output. A separate family keeps this slow cadence
 * apart from the full list Settings rereads while it is open.
 */
export const scheduledTaskFailureWatch = createEnvironmentRpcQueryAtomFamily(
  connectionAtomRuntime,
  {
    label: "environment-data:scheduler:failure-watch",
    tag: "scheduler.list",
    staleTimeMs: 30_000,
    refreshIntervalMs: 60_000,
  },
);
