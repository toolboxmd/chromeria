import { isCommandTask, type ScheduledTask } from "@t3tools/contracts";

/** What the last poll saw of each watched command task, by task id. */
export type CommandHealth = ReadonlyMap<
  string,
  { readonly streak: number; readonly lastSuccessfulRunId: string | undefined }
>;

/**
 * The command tasks that turned from passing to failing since `previous`, and the health to
 * compare the next poll with.
 *
 * Without `previous` (the first poll after mounting) it only records a baseline, so opening
 * the app never replays old failures. A task first seen after that counts as passing, so its
 * first-ever failure alerts. The list is sampled, so the newest run does not decide: a task
 * that is failing now alerts when it was passing at the last poll, or when a run passed since
 * then, shown by a new `lastSuccessfulRunId` even if the streak looks unchanged. Consecutive
 * failures, runs in progress and edits change neither, so they stay quiet.
 */
export function commandTasksTurnedFailing(
  previous: CommandHealth | null,
  tasks: ReadonlyArray<ScheduledTask>,
): { readonly health: CommandHealth; readonly failing: ReadonlyArray<ScheduledTask> } {
  const health = new Map<
    string,
    { readonly streak: number; readonly lastSuccessfulRunId: string | undefined }
  >();
  const failing: ScheduledTask[] = [];
  for (const task of tasks) {
    if (task.deleted || !isCommandTask(task.definition)) continue;
    const { failureStreak: streak, lastSuccessfulRunId } = task;
    health.set(task.id, { streak, lastSuccessfulRunId });
    if (previous === null || streak === 0) continue;
    const before = previous.get(task.id);
    if (
      before === undefined ||
      before.streak === 0 ||
      before.lastSuccessfulRunId !== lastSuccessfulRunId
    )
      failing.push(task);
  }
  return { health, failing };
}
