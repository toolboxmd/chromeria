import * as Notifications from "expo-notifications";

import { setAndroidThreadOnScreen } from "./androidNotifications";
import { foregroundNotificationBehavior } from "./foregroundNotificationBehavior";
import { routeAgentNotificationResponseOnce, threadDeepLinkOnScreen } from "./notificationPayload";
import { consumeLastAgentNotificationResponse } from "./notificationResponseConsumer";

let deepLinkOnScreen: string | null = null;

/** Called with the workspace pathname on every root navigation state change. */
export function setAgentNotificationPathname(pathname: string): void {
  deepLinkOnScreen = threadDeepLinkOnScreen(pathname);
  setAndroidThreadOnScreen(deepLinkOnScreen);
}

/**
 * App-lifetime notification routing: foreground presentation suppresses the
 * thread already on screen, and taps (including the cold-start tap) open the
 * notification's deep link through `navigate`.
 */
let launchResponseRouted: Promise<void> = Promise.resolve();

/** Fork: settles once the notification that launched the app, if any, has been routed. */
export function agentNotificationLaunchRouted(): Promise<void> {
  return launchResponseRouted;
}

export function installAgentNotificationNavigation(navigate: (href: string) => void): () => void {
  const handledResponseIds = new Set<string>();
  Notifications.setNotificationHandler({
    handleNotification: (notification) =>
      Promise.resolve(foregroundNotificationBehavior(notification, deepLinkOnScreen)),
  });

  const handleResponse = (response: Notifications.NotificationResponse): void => {
    routeAgentNotificationResponseOnce({ handledResponseIds, response, navigate });
  };
  const subscription = Notifications.addNotificationResponseReceivedListener(handleResponse);
  launchResponseRouted = consumeLastAgentNotificationResponse({
    getLastResponse: () => Notifications.getLastNotificationResponseAsync(),
    clearLastResponse: () => Notifications.clearLastNotificationResponseAsync(),
    handleResponse,
  });

  return () => {
    subscription.remove();
    Notifications.setNotificationHandler(null);
  };
}
