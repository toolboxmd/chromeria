import { expect } from "e2e";

import { answerIssueListPages, releaseIssueReads } from "./issue-list-pages.ts";
import { test, WEB } from "./t3.ts";

test(
  "A failed counts read leaves no open total marked as being read while rows still wait",
  WEB,
  async ({ t3, app, browser, screen }) => {
    await t3.project("demo");
    await answerIssueListPages(
      browser,
      // Every open Issue; the reread Refresh starts is held back, as a server that is away
      // holds its pages.
      { first: [{ issues: [3, 2, 1] }, { issues: [3, 2, 1], held: true }] },
      // The counts answer once, and the read Refresh starts fails.
      { counts: [{ open: 3, filed: 4, closed: 2 }, "failure"] },
    );
    const says = (text: string) => screen.getByText(text, { exact: true });

    await t3.open("/pull-requests?view=issues");
    await says("Open Issue 3").waitFor({ timeout: 60_000 });
    await expect(says("Last 7 days: 4 filed, 2 closed")).toBeVisible();
    await expect(says("3 open Issues")).toBeVisible();

    await screen.getByRole("button", "Refresh Issues", { exact: true }).tap();
    try {
      // The counts read has settled without an answer; the rows are still being read again.
      await expect(says("Open Issues and last 7 days: unknown")).toBeVisible();
      await expect(says("3 Issues")).toBeVisible();
      await expect(says("~3 open Issues")).toHaveCount(0);
      await expect(says("Open Issue 3")).toBeVisible();
    } finally {
      await app.screenshot("counts-failed-rows-waiting");
    }
    // Read to their end again, the rows are every open Issue and state the total themselves.
    await releaseIssueReads(browser);
    await expect(says("3 open Issues")).toBeVisible();
    await expect(says("Last 7 days: unknown")).toBeVisible();
  },
);
