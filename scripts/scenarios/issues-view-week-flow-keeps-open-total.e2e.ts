import { expect } from "e2e";

import { answerIssueListPages, releaseIssueReads } from "./issue-list-pages.ts";
import { test, WEB } from "./t3.ts";

test(
  "The Issues view shows the week's filed and closed, and keeps the open total under a search",
  WEB,
  async ({ t3, app, browser, screen }) => {
    await t3.project("demo");
    await answerIssueListPages(
      browser,
      // Every open Issue; the same again for Refresh, held back; then the one a search matches,
      // held back too, which later reads repeat.
      {
        first: [
          { issues: [3, 2, 1] },
          { issues: [3, 2, 1], held: true },
          { issues: [2], matches: 1, held: true },
          { issues: [2], matches: 1 },
        ],
      },
      // The first counts read, the one Refresh starts and the scenario holds back, and a third
      // that fails.
      {
        counts: [
          { open: 3, filed: 4, closed: 2 },
          { open: 30, filed: 40, closed: 20, held: true },
          "failure",
        ],
      },
    );
    const says = (text: string) => screen.getByText(text, { exact: true });
    const refresh = screen.getByRole("button", "Refresh Issues", { exact: true });

    await t3.open("/pull-requests?view=issues");
    await says("Open Issue 3").waitFor({ timeout: 60_000 });
    try {
      await expect(says("Last 7 days: 4 filed, 2 closed")).toBeVisible();
      await expect(says("3 open Issues")).toBeVisible();
      await app.screenshot("week-flow");

      // While Refresh reads the rows and the counts again, the numbers stay, marked not final.
      await refresh.tap();
      await expect(says("Last 7 days: ~4 filed, ~2 closed")).toBeVisible();
      await expect(says("~3 open Issues")).toBeVisible();
      await app.screenshot("refreshing");
      await releaseIssueReads(browser);
      await expect(says("Last 7 days: 40 filed, 20 closed")).toBeVisible();
      await expect(says("3 open Issues")).toBeVisible();

      // A minute on, the counts are stale enough that mounting their read again would repeat it.
      await browser.evaluate(() => {
        const now = Date.now;
        // @effect-diagnostics-next-line globalDate:off - This callback moves the browser's own clock.
        Date.now = () => now() + 60_000;
        return null;
      });
      // A search narrows the rows to a part of the open total, and starts no counts read: one
      // would fail and read as unknown.
      await screen.getByRole("searchbox", "Search Issues", { exact: true }).fill("two");
      await says("Open Issue 3").waitFor({ state: "hidden" });
      await releaseIssueReads(browser);
      await expect(says("1 of 30 open Issues")).toBeVisible();
      await expect(says("Last 7 days: 40 filed, 20 closed")).toBeVisible();
      await app.screenshot("searched");

      // A counts read that fails says so; the rows stay, and no stale total is claimed.
      await refresh.tap();
      await expect(says("Open Issues and last 7 days: unknown")).toBeVisible();
      await expect(says("1 Issue")).toBeVisible();
      await expect(says("Open Issue 2")).toBeVisible();
    } finally {
      await app.screenshot("unknown");
    }
  },
);
