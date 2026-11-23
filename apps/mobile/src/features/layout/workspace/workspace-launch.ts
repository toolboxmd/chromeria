import { NAVIGATION_TAB_ORDER, type NavigationTab } from "./navigation-tab-order";

export interface WorkspaceLaunch {
  /** The tab to open on; unset keeps the default. */
  readonly tab: NavigationTab | undefined;
  /** The Promachos conversation to reopen over its list. */
  readonly promachosConversation: {
    readonly environmentId: string;
    readonly threadId: string;
  } | null;
}

/**
 * Fork (toolboxmd/chromeria#898): a cold start without a link reopens the tab used last, and the
 * Promachos tab its last conversation; with nothing remembered it opens on the Promachos tab. A
 * link, notification or quick action wins: it opens as before, on the default tab.
 */
export function resolveWorkspaceLaunch(input: {
  readonly launchIntent: boolean;
  readonly lastTab: unknown;
  /** Tabs the user hid; a hidden tab is not reopened. */
  readonly hiddenTabs: ReadonlyArray<unknown>;
  readonly promachosConversation:
    | { readonly environmentId: string; readonly threadId: string }
    | undefined;
}): WorkspaceLaunch {
  const tab =
    input.lastTab === undefined
      ? "promachos"
      : NAVIGATION_TAB_ORDER.find((candidate) => candidate === input.lastTab);
  if (input.launchIntent || tab === undefined || input.hiddenTabs.includes(tab))
    return { tab: undefined, promachosConversation: null };
  return {
    tab,
    promachosConversation: tab === "promachos" ? (input.promachosConversation ?? null) : null,
  };
}
