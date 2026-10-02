import type { EnvironmentId } from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import { CircleAlertIcon } from "lucide-react";
import { useEffect, useRef } from "react";

import { getClientSettings, useClientSettings } from "../hooks/useSettings";
import { type CommandHealth, commandTasksTurnedFailing } from "../scheduledCommandFailures";
import { useEnvironmentQuery } from "../state/query";
import { scheduledTaskFailureWatch } from "../state/scheduler";
import {
  hasDesktopNotifications,
  hasNotificationSound,
  playNotificationSound,
} from "../threadNotifications";
import { toastManager } from "./ui/toast";

/**
 * Alerts once when a scheduled command in this environment starts failing, through the same
 * preferences, sound, toast, desktop notification and badge as thread alerts. Rendered by the
 * thread notification coordinator, which owns the badge count.
 */
export function ScheduledCommandNotifications({
  environmentId,
  onNotification,
}: {
  environmentId: EnvironmentId;
  onNotification: (environmentId: EnvironmentId, notification: Notification) => void;
}) {
  const { data } = useEnvironmentQuery(
    scheduledTaskFailureWatch({ environmentId, input: { compact: true } }),
  );
  const mode = useClientSettings((settings) => settings.notificationMode);
  const inAppNotificationsEnabled = useClientSettings(
    (settings) => settings.inAppNotificationsEnabled,
  );
  const navigate = useNavigate();
  // Kept across reconnects, so a failure that began while offline alerts once, not again.
  const health = useRef<CommandHealth | null>(null);

  useEffect(() => {
    if (data === null) return;
    const { health: next, failing } = commandTasksTurnedFailing(health.current, data);
    health.current = next;
    const open = () =>
      void navigate({ to: "/settings/scheduled-tasks", search: { machine: environmentId } });
    for (const task of failing) {
      const title = "Scheduled command failed";
      if (hasNotificationSound(mode)) {
        void playNotificationSound("input", () =>
          hasNotificationSound(getClientSettings().notificationMode),
        );
      }
      const focused = document.visibilityState === "visible" && document.hasFocus();
      if (inAppNotificationsEnabled && focused) {
        const toastId = toastManager.add({
          type: "error",
          title,
          description: task.definition.title,
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
        !hasDesktopNotifications(mode) ||
        focused ||
        typeof Notification === "undefined" ||
        Notification.permission !== "granted"
      )
        continue;
      try {
        const notification = new Notification(title, {
          body: task.definition.title,
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
  }, [data, environmentId, inAppNotificationsEnabled, mode, navigate, onNotification]);

  return null;
}
