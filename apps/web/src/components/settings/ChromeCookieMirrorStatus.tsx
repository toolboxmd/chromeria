import { useEffect } from "react";
import { adoptDesktopBrowserProfiles } from "../../hooks/useSettings";
import { recordCookieMirrorStatus, useCookieMirrorLimitations } from "../../rpc/cookieMirrorStatus";
import { SettingsRow } from "./settingsLayout";

export function ChromeCookieMirrorStatus() {
  const limitations = useCookieMirrorLimitations();
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Keep desktop cookie-mirror metadata current through its IPC bridge while mounted.
  useEffect(() => {
    const bridge = window.desktopBridge?.cookieMirrorConnection;
    if (!bridge) return;
    let disposed = false;
    let pending = false;
    const refresh = async () => {
      if (pending) return;
      pending = true;
      try {
        const status = await bridge({ action: "status" });
        if (!disposed) {
          recordCookieMirrorStatus(status);
          if (status.profiles.length > 0) await adoptDesktopBrowserProfiles(status.profiles);
        }
      } catch {
        // Main owns diagnostics; this metadata read carries no cookie payload.
      }
      pending = false;
    };
    void refresh();
    const timer = setInterval(() => {
      void refresh();
    }, 5000);
    return () => {
      disposed = true;
      clearInterval(timer);
    };
  }, []);
  return (
    <SettingsRow
      title="Chrome login mirror"
      description="On macOS, Chrome logins follow the Mac while Chromeria runs. Device-bound logins require signing in separately in each environment."
    >
      {limitations.length > 0 ? (
        <ul aria-label="Device-bound sites" className="space-y-1 text-sm text-muted-foreground">
          {limitations.map((item) => (
            <li key={item.host}>
              {item.host}: {item.reason}
            </li>
          ))}
        </ul>
      ) : null}
    </SettingsRow>
  );
}
