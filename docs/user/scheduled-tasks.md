# Scheduled tasks

A scheduled task sends a prompt on a schedule and keeps the work going until its
outcome check passes. A finished turn is not enough: a run is done only when the
check exits 0 and no Drafter the run started is still pending.

## Create a task

1. On web or desktop, open **Settings → Scheduled tasks** and choose **New task**.
2. Write the prompt and choose the target: a new thread in a project, or an
   existing thread to continue.
3. Choose the Prism role the work runs in. For **Worker**, also choose the lane.
4. Choose the schedule: set times on chosen weekdays, every N minutes, or once.
5. Write the outcome check and why it proves the work is done.

Agents can manage tasks too, with the scheduled task tools.

## Outcome checks

The check is a shell command the server runs after each turn. It runs in the
project root for a new thread. For an existing thread it runs in the thread's
worktree, or in the project root when the thread has none. It can use `{date}`,
`{run_id}` and `{task_id}`, and times out after 30 seconds.

The check must fail when you create the task: a check that already passes is
refused. The running agent sees the check's output but cannot change the check.
You or another agent can add a new version with a reason. It applies to future
runs only, and **Revert to this** in the task's check history brings back an
older version the same way. Every version shows who added it and when.

## Times and windows

A set time is a window, not an exact minute. By default the server picks the
least busy minute up to 30 minutes before or after the time you name, so tasks
asking for the same time spread out. The task shows the chosen minute and the
tasks already near it. Set the window to 0 to run at the exact minute.

## When a run fails

- A run that never started, or failed before doing anything, starts again after
  30 seconds, then 1, 5, 15 and 60 minutes, moving down the role's model list.
- A run that did some work is never restarted from scratch. The thread gets a
  "continue" with the error or the check output. A usage limit waits for its reset.
- When the retries run out, the task shows **Needs you** with the reason. The
  run stays unfinished: the next scheduled time tries the same run again, in
  the same thread and with the same check.
- After the server was off, a task runs once for the latest missed time. A time
  that arrives while the previous run is still unfinished is skipped.

## Manage a task

- **Pause** stops new runs and the scheduler's retries and continues, without
  cancelling the current thread or its turn. **Resume** picks the work up again.
- **Run now** starts a run. It waits while a run is in progress, and a paused
  task must be resumed first. On a task that needs you it shows **Resume run**
  and continues the same run in the same thread. If that thread was stopped, it
  is refused: send the thread a message to take it back up, and resume the run
  once that work has finished.
- **Delete** removes the task from the list. It waits while a run is in progress.
  On a task that needs you it is refused while anything the run started is still
  live or pending. The run is never marked done, and its check history is kept.

Scheduled tasks belong to one environment. When Settings shows all
environments or a project, the page asks you to choose the environment whose
tasks to manage.
