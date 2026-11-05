import {
  type CommandHealth,
  commandTasksTurnedFailing,
} from "@t3tools/client-runtime/scheduled-task-fork";
import type { EnvironmentId, ScheduledTask } from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import { CircleAlertIcon } from "lucide-react";
import { useEffect, useRef } from "react";

import { getClientSettings, useClientSettings } from "../hooks/useSettings";
import { useEnvironmentQuery } from "../state/query";
import { serverEnvironment } from "../state/server";
import {
  hasDesktopNotifications,
  hasNotificationSound,
  playNotificationSound,
} from "../threadNotifications";
import { toastManager } from "./ui/toast";

/**
 * Fork (toolboxmd/chromeria#174, from v1): alerts once when a scheduled
 * shell command in this environment starts failing, through the same
 * preferences, sound, toast, desktop notification and badge as thread alerts.
 * Shares presentation with the imperative thread notification coordinator.
 */
export function ScheduledCommandNotifications({
  environmentId,
  onNotification,
}: {
  environmentId: EnvironmentId;
  onNotification: (environmentId: EnvironmentId, notification: Notification) => void;
}) {
  const { data } = useEnvironmentQuery(
    serverEnvironment.scheduledTasksLive({ environmentId, input: {} }),
  );
  const mode = useClientSettings((settings) => settings.notificationMode);
  const inAppNotificationsEnabled = useClientSettings(
    (settings) => settings.inAppNotificationsEnabled,
  );
  const navigate = useNavigate();
  // Kept across reconnects, so a failure that began while offline alerts once, not again.
  const health = useRef<CommandHealth | null>(null);

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- presents imperative toasts and desktop notifications when command health changes
  useEffect(() => {
    if (data === null) return;
    const { health: next, failing } = commandTasksTurnedFailing(health.current, data.tasks);
    health.current = next;
    presentScheduledCommandFailures(environmentId, failing, {
      notificationMode: mode,
      inAppNotificationsEnabled,
      open: () => void navigate({ to: "/settings/scheduled-tasks", search: { environmentId } }),
      onNotification,
    });
  }, [data, environmentId, inAppNotificationsEnabled, mode, navigate, onNotification]);

  return null;
}

export function presentScheduledCommandFailures(
  environmentId: EnvironmentId,
  failing: ReadonlyArray<Pick<ScheduledTask, "id" | "title">>,
  {
    notificationMode,
    inAppNotificationsEnabled,
    open,
    onNotification,
  }: {
    notificationMode: ReturnType<typeof getClientSettings>["notificationMode"];
    inAppNotificationsEnabled: boolean;
    open: () => void;
    onNotification: (environmentId: EnvironmentId, notification: Notification) => void;
  },
) {
  for (const task of failing) {
    const title = "Scheduled command failed";
    if (hasNotificationSound(notificationMode)) {
      void playNotificationSound("input", () =>
        hasNotificationSound(getClientSettings().notificationMode),
      );
    }
    const focused = document.visibilityState === "visible" && document.hasFocus();
    if (inAppNotificationsEnabled && focused) {
      const toastId = toastManager.add({
        type: "error",
        title,
        description: task.title,
        data: {
          hideCopyButton: true,
          leadingIcon: (
            <CircleAlertIcon aria-hidden className="size-4 text-destructive-foreground" />
          ),
        },
        actionProps: {
          children: "Open scheduled tasks",
          onClick: () => {
            toastManager.close(toastId);
            open();
          },
        },
      });
      continue;
    }
    if (
      !hasDesktopNotifications(notificationMode) ||
      focused ||
      typeof Notification === "undefined" ||
      Notification.permission !== "granted"
    )
      continue;
    try {
      const notification = new Notification(title, {
        body: task.title,
        tag: `${environmentId}:scheduled:${task.id}`,
        silent: true,
      });
      onNotification(environmentId, notification);
      notification.addEventListener("click", () => {
        notification.close();
        window.focus();
        open();
      });
    } catch {
      // Some browsers expose Notification but reject desktop presentation.
    }
  }
}
