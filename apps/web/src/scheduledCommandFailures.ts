import { isCommandTask, type ScheduledTask } from "@t3tools/contracts";

/** Each watched command task's failure streak at the last poll, by task id. */
export type CommandHealth = ReadonlyMap<string, number>;

/**
 * The command tasks that turned from passing to failing since `previous`, and the health to
 * compare the next poll with.
 *
 * Without `previous` (the first poll after mounting) it only records a baseline, so opening
 * the app never replays old failures. A task first seen after that counts as passing, so its
 * first-ever failure alerts. The list is sampled, so the task's streak decides, not its newest
 * run: a run in progress keeps the streak, several failures between polls alert once, and a
 * streak that shrank means a run passed in between and the task failed again.
 */
export function commandTasksTurnedFailing(
  previous: CommandHealth | null,
  tasks: ReadonlyArray<ScheduledTask>,
): { readonly health: CommandHealth; readonly failing: ReadonlyArray<ScheduledTask> } {
  const health = new Map<string, number>();
  const failing: ScheduledTask[] = [];
  for (const task of tasks) {
    if (task.deleted || !isCommandTask(task.definition)) continue;
    const streak = task.failureStreak;
    health.set(task.id, streak);
    if (previous === null || streak === 0) continue;
    const before = previous.get(task.id) ?? 0;
    if (before === 0 || streak < before) failing.push(task);
  }
  return { health, failing };
}
