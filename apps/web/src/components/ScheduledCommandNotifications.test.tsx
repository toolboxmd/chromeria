import type { ClientSettings } from "@t3tools/contracts/settings";
import { EnvironmentId } from "@t3tools/contracts";
import { act } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

const state = vi.hoisted(() => ({
  mode: "off" as ClientSettings["notificationMode"],
  inApp: true,
  focused: true,
  visible: "visible",
  data: null as { tasks: ReadonlyArray<unknown> } | null,
  add: vi.fn(
    (_toast: { title: string; description: string; actionProps: { onClick: () => void } }) =>
      "toast-1",
  ),
  close: vi.fn(),
  navigate: vi.fn(),
  sound: vi.fn(),
  notification: vi.fn(function (_title: string, options: NotificationOptions) {
    return Object.assign(new EventTarget(), { tag: options.tag, close: vi.fn() });
  }),
}));

// The live task list, as the environment's scheduled task subscription delivers it.
vi.mock("../state/query", () => ({ useEnvironmentQuery: () => ({ data: state.data }) }));
vi.mock("../state/server", () => ({ serverEnvironment: { scheduledTasksLive: vi.fn() } }));
vi.mock("@tanstack/react-router", () => ({ useNavigate: () => state.navigate }));
vi.mock("../hooks/useSettings", () => ({
  useClientSettings: (
    select: (
      settings: Pick<ClientSettings, "notificationMode" | "inAppNotificationsEnabled">,
    ) => unknown,
  ) => select({ notificationMode: state.mode, inAppNotificationsEnabled: state.inApp }),
  getClientSettings: () => ({ notificationMode: state.mode }),
}));
vi.mock("../threadNotifications", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../threadNotifications")>()),
  playNotificationSound: state.sound,
}));
vi.mock("./ui/toast", () => ({ toastManager: { add: state.add, close: state.close } }));

import { ScheduledCommandNotifications } from "./ScheduledCommandNotifications";

const environmentId = EnvironmentId.make("env-1");
const onNotification = vi.fn();
let renderer: ReactTestRenderer | undefined;

/** A command task as the live list carries it: no output, only its health. */
const commandTask = (failureStreak: number, lastSuccessfulRunId: string | null) => ({
  id: "task-backup",
  title: "Nightly backup",
  command: { command: "make backup", run: null, failureStreak, lastSuccessfulRunId },
});

async function receive(...tasks: ReadonlyArray<ReturnType<typeof commandTask>>) {
  state.data = { tasks };
  await act(() => {
    const element = (
      <ScheduledCommandNotifications
        environmentId={environmentId}
        onNotification={onNotification}
      />
    );
    if (renderer) renderer.update(element);
    else renderer = create(element);
  });
}

beforeEach(() => {
  vi.clearAllMocks();
  Object.assign(state, { mode: "off", inApp: true, focused: true, visible: "visible", data: null });
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal("window", Object.assign(new EventTarget(), { focus: vi.fn() }));
  vi.stubGlobal("document", {
    get visibilityState() {
      return state.visible;
    },
    hasFocus: () => state.focused,
  });
  vi.stubGlobal("Notification", Object.assign(state.notification, { permission: "granted" }));
});

afterEach(async () => {
  await act(() => renderer?.unmount());
  renderer = undefined;
  vi.unstubAllGlobals();
});

describe("scheduled command notifications", () => {
  it("alert once in the app when a command turns from passing to failing, and open scheduled tasks", async () => {
    state.mode = "sound";
    await receive(commandTask(0, "run:1"));
    expect(state.add).not.toHaveBeenCalled();
    await receive(commandTask(1, "run:1"));
    // The same failing state again, as the next live emission brings it, stays quiet.
    await receive(commandTask(1, "run:1"));
    expect(state.add).toHaveBeenCalledTimes(1);
    expect(state.sound).toHaveBeenCalledTimes(1);
    expect(state.notification).not.toHaveBeenCalled();
    const toast = state.add.mock.calls[0]?.[0];
    expect(toast?.title).toBe("Scheduled command failed");
    expect(toast?.description).toBe("Nightly backup");
    toast?.actionProps.onClick();
    expect(state.close).toHaveBeenCalledWith("toast-1");
    expect(state.navigate).toHaveBeenCalledWith({
      to: "/settings/scheduled-tasks",
      search: { environmentId },
    });
  });

  it("show a desktop notification in the background and hand it to the badge count", async () => {
    state.mode = "notifications";
    state.focused = false;
    state.visible = "hidden";
    await receive(commandTask(0, "run:1"));
    await receive(commandTask(1, "run:1"));
    expect(state.add).not.toHaveBeenCalled();
    expect(state.notification).toHaveBeenCalledTimes(1);
    expect(state.notification).toHaveBeenCalledWith("Scheduled command failed", {
      body: "Nightly backup",
      tag: "env-1:scheduled:task-backup",
      silent: true,
    });
    expect(onNotification).toHaveBeenCalledTimes(1);
    const [notifiedEnvironment, notification] = onNotification.mock.calls[0]!;
    expect(notifiedEnvironment).toBe(environmentId);
    expect(notification.tag).toBe("env-1:scheduled:task-backup");
    notification.dispatchEvent(new Event("click"));
    expect(notification.close).toHaveBeenCalled();
    expect(state.navigate).toHaveBeenCalledWith({
      to: "/settings/scheduled-tasks",
      search: { environmentId },
    });
  });
});
