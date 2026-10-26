import {
  CommandId,
  type NodeId,
  type OrchestrationV2Subagent,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";

/**
 * Run groups: one Run N delegation starts several Colors of a
 * read-only role as tasks of one group. The group is the set of tasks that
 * share a `runGroupId`, all recorded by one `delegated_task.group.request`
 * command, so its membership never changes after that command commits.
 */

/** One member's place in its group's command: its ids derive from `idSeed`. */
export interface RunGroupMember {
  readonly groupId: NodeId;
  readonly index: number;
  readonly idSeed: CommandId;
}

export function runGroupMember(groupCommandId: CommandId, groupId: NodeId, index: number) {
  return {
    groupId,
    index,
    idSeed: CommandId.make(`${groupCommandId}:member:${index}`),
  } satisfies RunGroupMember;
}

function isTerminal(status: OrchestrationV2Subagent["status"]) {
  return (
    status === "completed" ||
    status === "failed" ||
    status === "cancelled" ||
    status === "interrupted"
  );
}

export function runGroupMembers(
  subagents: ReadonlyArray<OrchestrationV2Subagent>,
  groupId: NodeId,
): ReadonlyArray<OrchestrationV2Subagent> {
  return subagents.filter((task) => task.origin === "app_owned" && task.runGroupId === groupId);
}

/**
 * What a terminal result does to the parent's wake. A task outside a group
 * wakes as before. A member holds while another member still runs; the last
 * one to finish releases every member still held, so the parent wakes once.
 * A member already in a delivery was released by a sibling and owes nothing.
 */
export type RunGroupWake =
  | { readonly type: "single" }
  | { readonly type: "hold" }
  | { readonly type: "release"; readonly siblings: ReadonlyArray<OrchestrationV2Subagent> };

export function runGroupWake(
  subagents: ReadonlyArray<OrchestrationV2Subagent>,
  task: OrchestrationV2Subagent,
): RunGroupWake {
  if (task.runGroupId === undefined) return { type: "single" };
  if (task.completionDelivery !== undefined) return { type: "hold" };
  const siblings = runGroupMembers(subagents, task.runGroupId).filter(
    (member) => member.id !== task.id,
  );
  if (siblings.some((member) => !isTerminal(member.status))) return { type: "hold" };
  return {
    type: "release",
    siblings: siblings.filter((member) => member.completionDelivery === undefined),
  };
}

/**
 * The wake text for a group: each member's status and result, since the
 * group wakes the parent only once.
 */
export function runGroupWakeDetail(
  parent: Pick<OrchestrationV2ThreadProjection, "subagents">,
  groupId: NodeId,
): string {
  const members = runGroupMembers(parent.subagents, groupId)
    .map(
      (member) =>
        `[${member.id} · ${member.model ?? member.providerInstanceId}] ${member.status}\n${member.result ?? "(no result)"}`,
    )
    .join("\n\n");
  return `Run group ${groupId} finished. Each member's status and result:\n\n${members}\n\ntask_status with taskId ${groupId} returns them again.`;
}
