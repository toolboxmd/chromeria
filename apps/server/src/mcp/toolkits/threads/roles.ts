import {
  LEGACY_PRISM_ROLE_KEYS,
  PRISM_ROLES,
  type PrismModelPreference,
  type PrismRole,
  type PrismRoleKit,
  type ServerProvider,
} from "@t3tools/contracts";

import { isSubagentThreadId } from "./subagentThreadId.ts";

/**
 * Prism roles on T3 threads (toolboxmd/model-router#115).
 *
 * A thread the user starts (no parent) is the planner. A child started in a
 * role carries it at the front of its id suffix, `sub.<parent>.<role>-<rand>`,
 * so the role survives restarts like the parent link does; children spawned
 * as `correction` or `recovery` read as Retry and Escalation. A child without
 * a role prefix (direct spawns, router threads) is unassigned. Every thread
 * may use every thread tool, whatever its role.
 */
export function prismRoleSuffix(role: PrismRole, random: string): string {
  return `${role}-${random}`;
}

export function threadRoleOf(threadId: string): PrismRole | "unassigned" {
  if (!isSubagentThreadId(threadId)) return "planner";
  const suffix = threadId.slice(threadId.lastIndexOf(".") + 1);
  const prefix = suffix.slice(0, suffix.indexOf("-"));
  if ((PRISM_ROLES as readonly string[]).includes(prefix)) return prefix as PrismRole;
  return LEGACY_PRISM_ROLE_KEYS[prefix] ?? "unassigned";
}

/**
 * A provider instance is blocked while any usage window is exhausted and
 * has not reset yet. A window at 100 % without `resetsAt` blocks until the
 * next refresh replaces the reading.
 */
export function isProviderBlocked(provider: ServerProvider, nowMs: number): boolean {
  return (provider.usageLimits?.windows ?? []).some(
    (window) =>
      window.usedPercent >= 100 &&
      (window.resetsAt === undefined || Date.parse(window.resetsAt) > nowMs),
  );
}

function offersModel(provider: ServerProvider, model: string): boolean {
  return provider.models.some(
    (candidate) => candidate.slug === model || (candidate.aliases ?? []).includes(model),
  );
}

/**
 * The first entry of a role's lane list whose instance is enabled in
 * Providers, offers the model and is not blocked by a usage limit, or the
 * reason none is. Later entries are the fallbacks.
 */
export function pickRoleModel(
  preferences: ReadonlyArray<PrismModelPreference>,
  providers: ReadonlyArray<ServerProvider>,
  nowMs: number,
): { readonly pick: PrismModelPreference } | { readonly refusal: string } {
  const skipped: string[] = [];
  for (const preference of preferences) {
    const provider = providers.find((candidate) => candidate.instanceId === preference.instanceId);
    const label = `${preference.instanceId}/${preference.model}`;
    if (!provider || !provider.enabled || provider.availability === "unavailable") {
      skipped.push(`${label} (instance disabled)`);
    } else if (!offersModel(provider, preference.model)) {
      skipped.push(`${label} (model not offered)`);
    } else if (isProviderBlocked(provider, nowMs)) {
      skipped.push(`${label} (usage limit reached)`);
    } else {
      return { pick: preference };
    }
  }
  return { refusal: `No eligible model for this role and lane: ${skipped.join(", ")}.` };
}

/** The child's first message: the role's instructions and skills, then the task. */
export function roleTaskMessage(kit: PrismRoleKit, task: string): string {
  const parts = [
    kit.instructions,
    kit.skills.length > 0 ? `Use these skills: ${kit.skills.join(", ")}.` : "",
    task,
  ];
  return parts.filter((part) => part.length > 0).join("\n\n");
}

/**
 * Provider option id that carries reasoning effort, per driver. Codex and
 * Grok advertise `reasoningEffort`, OpenCode advertises `variant`, and
 * Claude, Cursor and Antigravity read `effort`. ModelSelection options are
 * free-form id/value pairs that adapters ignore when unknown, so sending
 * `effort` to Antigravity (which has no effort control) is a harmless no-op.
 */
export function effortOptionId(driverKind: string): string {
  switch (driverKind) {
    case "codex":
    case "grok":
      return "reasoningEffort";
    case "opencode":
      return "variant";
    default:
      return "effort";
  }
}
