import { ProjectId, type ScheduledTask, type TaskRun } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { act } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

const state = vi.hoisted(() => ({
  mode: "notifications",
  inApp: false,
  environmentIds: ["one", "two"],
  /** The compact list each environment's watch query currently holds; absent until it loads. */
  lists: new Map<string, ReadonlyArray<unknown>>(),
  watched: [] as Array<{ environmentId: string; input: unknown }>,
  toast: vi.fn((_toast: { actionProps: { onClick: () => void } }) => "toast-1"),
  close: vi.fn(),
  navigate: vi.fn(),
  sound: vi.fn(),
  badge: vi.fn(),
}));
// Threads stay quiet: these tests watch only scheduled commands.
vi.mock("@effect/atom-react", () => ({
  useAtomValue: () => ({ status: "connecting", snapshot: Option.none() }),
}));
vi.mock("@tanstack/react-router", () => ({
  useNavigate: () => state.navigate,
  useParams: () => ({}),
}));
vi.mock("./ui/toast", () => ({ toastManager: { add: state.toast, close: state.close } }));
vi.mock("../state/shell", () => ({ environmentShell: { stateValueAtom: vi.fn() } }));
vi.mock("../state/environments", () => ({
  useEnvironments: () => ({
    environments: state.environmentIds.map((environmentId) => ({ environmentId })),
  }),
}));
vi.mock("../state/scheduler", () => ({
  scheduledTaskFailureWatch: (target: { environmentId: string; input: unknown }) => {
    state.watched.push(target);
    return target.environmentId;
  },
}));
vi.mock("../state/query", () => ({
  useEnvironmentQuery: (environmentId: string) => ({
    data: state.lists.get(environmentId) ?? null,
  }),
}));
vi.mock("../hooks/useSettings", () => ({
  useClientSettings: (
    select: (settings: { notificationMode: string; inAppNotificationsEnabled: boolean }) => unknown,
  ) => select({ notificationMode: state.mode, inAppNotificationsEnabled: state.inApp }),
  getClientSettings: () => ({ notificationMode: state.mode }),
}));
vi.mock("../threadNotifications", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../threadNotifications")>()),
  playNotificationSound: state.sound,
  unlockNotificationAudio: vi.fn(),
  setNotificationBadge: state.badge,
}));

import { ThreadNotificationCoordinator } from "./ThreadNotificationCoordinator";

class TestNotification extends EventTarget {
  static permission = "granted";
  static sent: TestNotification[] = [];
  close = vi.fn();
  get tag() {
    return this.options.tag ?? "";
  }
  constructor(
    readonly title: string,
    readonly options: NotificationOptions,
  ) {
    super();
    TestNotification.sent.push(this);
  }
}

const definition = {
  kind: "command" as const,
  title: "Backup",
  projectId: ProjectId.make("project-1"),
  command: "./backup.sh",
  schedule: { kind: "interval" as const, minutes: 5 },
};

/** A task as the compact list returns it: only its newest run, without output. */
function task(
  failureStreak: number,
  newest: TaskRun["status"] | null = failureStreak > 0 ? "needs-you" : "done",
  patch: Partial<ScheduledTask> = {},
): ScheduledTask {
  const run: TaskRun = {
    definition,
    checkCwd: "/repo",
    id: `task-1:${failureStreak}:${newest}`,
    slot: "2026-10-02T08:00:00.000Z",
    threadId: null,
    status: newest ?? "done",
    processId: "p",
    originSequence: 0,
    sendIndex: 0,
    attempt: 0,
    hasWork: false,
    leaseUntil: 0,
    retryAt: null,
    dispatchedAt: "2026-10-02T08:00:00.000Z",
    observedTurnId: null,
    error: null,
    check: null,
    drafterIds: [],
  };
  return {
    checkCwd: "/repo",
    id: "task-1",
    revision: 1,
    definition,
    createdAt: "2026-10-01T00:00:00.000Z",
    updatedAt: "2026-10-01T00:00:00.000Z",
    paused: false,
    deleted: false,
    checks: [],
    choices: [],
    consumedSlot: null,
    runs: newest === null ? [] : [run],
    failureStreak,
    lastError: failureStreak > 0 ? "Exit 3" : null,
    ...patch,
  };
}

let renderer: ReactTestRenderer | undefined;
let focused = false;

async function poll(environmentId: string, ...tasks: ScheduledTask[]) {
  state.lists.set(environmentId, tasks);
  await act(async () => {
    if (renderer) renderer.update(<ThreadNotificationCoordinator />);
    else renderer = create(<ThreadNotificationCoordinator />);
  });
}

const alerts = () => TestNotification.sent.map((notification) => notification.tag);

beforeEach(() => {
  vi.clearAllMocks();
  Object.assign(state, {
    mode: "notifications",
    inApp: false,
    environmentIds: ["one", "two"],
    lists: new Map(),
    watched: [],
  });
  focused = false;
  TestNotification.permission = "granted";
  TestNotification.sent = [];
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal("Notification", TestNotification);
  vi.stubGlobal("window", Object.assign(new EventTarget(), { focus: vi.fn() }));
  vi.stubGlobal(
    "document",
    Object.assign(new EventTarget(), {
      hasFocus: () => focused,
      visibilityState: "visible",
    }),
  );
});

afterEach(async () => {
  await act(async () => renderer?.unmount());
  renderer = undefined;
  vi.unstubAllGlobals();
});

describe("scheduled command failure notifications", () => {
  it("watches every environment through the compact list", async () => {
    await poll("one");
    expect(state.watched).toContainEqual({ environmentId: "one", input: { compact: true } });
    expect(state.watched).toContainEqual({ environmentId: "two", input: { compact: true } });
  });

  it("does not replay failures that already existed when the app opened", async () => {
    await poll("one", task(4));
    await poll("one", task(5));
    expect(alerts()).toEqual([]);
    expect(state.badge.mock.calls.every(([count]) => count === 0)).toBe(true);
  });

  it("alerts once per change into failure and again after a pass clears it", async () => {
    await poll("one", task(0));
    await poll("one", task(1));
    expect(alerts()).toEqual(["one:scheduled:task-1"]);
    expect(TestNotification.sent[0]!.title).toBe("Scheduled command failed");
    expect(TestNotification.sent[0]!.options.body).toBe("Backup");
    expect(state.badge).toHaveBeenLastCalledWith(1);
    await poll("one", task(2));
    await poll("one", task(2, "running"));
    expect(alerts()).toHaveLength(1);
    await poll("one", task(0));
    await poll("one", task(1));
    expect(alerts()).toHaveLength(2);
  });

  it("alerts for failures that happened between polls", async () => {
    await poll("one", task(0));
    // Two failed runs between polls: the streak, not the newest run, shows the change.
    await poll("one", task(2, "running"));
    expect(alerts()).toHaveLength(1);
    // A shorter streak means a run passed in between and the task failed again.
    await poll("one", task(1));
    expect(alerts()).toHaveLength(2);
  });

  it("counts a task's first-ever failure, and never alerts for agent tasks", async () => {
    await poll("one");
    await poll("one", task(1));
    expect(alerts()).toHaveLength(1);
    const agent = task(1, "needs-you", {
      id: "agent-1",
      definition: {
        title: "Report",
        prompt: "Write it.",
        target: { kind: "new-thread", projectId: ProjectId.make("project-1") },
        role: "worker",
        schedule: definition.schedule,
      },
    });
    await poll("two");
    await poll("two", agent);
    expect(alerts()).toHaveLength(1);
  });

  it("counts alerts from every environment in the shared badge", async () => {
    await poll("one", task(0));
    await poll("two", task(0));
    await poll("one", task(1));
    await poll("two", task(1));
    expect(alerts()).toEqual(["one:scheduled:task-1", "two:scheduled:task-1"]);
    expect(state.badge).toHaveBeenLastCalledWith(2);
    TestNotification.sent[1]!.dispatchEvent(new Event("click"));
    expect(state.navigate).toHaveBeenCalledWith({
      to: "/settings/scheduled-tasks",
      search: { machine: "two" },
    });
    focused = true;
    window.dispatchEvent(new Event("focus"));
    expect(state.badge).toHaveBeenLastCalledWith(0);
  });

  it("follows the notification preferences", async () => {
    state.mode = "off";
    await poll("one", task(0));
    expect(state.watched).toEqual([]);

    state.mode = "sound";
    await poll("one", task(0));
    await poll("one", task(1));
    expect(state.sound).toHaveBeenCalledOnce();
    expect(alerts()).toEqual([]);

    state.mode = "off";
    state.inApp = true;
    focused = true;
    await poll("one", task(0));
    await poll("one", task(1));
    expect(state.toast).toHaveBeenCalledOnce();
    expect(state.sound).toHaveBeenCalledOnce();
    state.toast.mock.calls[0]![0].actionProps.onClick();
    expect(state.navigate).toHaveBeenCalledWith({
      to: "/settings/scheduled-tasks",
      search: { machine: "one" },
    });
    expect(alerts()).toEqual([]);
  });
});
