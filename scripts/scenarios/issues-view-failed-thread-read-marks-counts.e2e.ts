import { expect } from "e2e";

import { answerIssueListPages } from "./issue-list-pages.ts";
import { test, WEB } from "./t3.ts";

test(
  "A failed linked-thread read marks the Issue counts as unsettled until Retry",
  WEB,
  async ({ t3, app, browser, screen }) => {
    await t3.project("demo");
    // Every Issue is read, but the first read of their linked threads fails.
    await answerIssueListPages(browser, { first: [{ issues: [2, 1] }] }, 1);
    const count = browser.locator("button[aria-expanded] .tabular-nums");

    await t3.open("/pull-requests?view=issues");
    await screen.getByText("Linked threads on", { exact: false }).waitFor({ timeout: 60_000 });
    // A status filter makes the total depend on threads too; without one it is exact.
    await expect(screen.getByText("2 Issues", { exact: true })).toBeVisible();
    await expect(count).toHaveText("~2");
    await app.screenshot("failed-thread-read");

    await screen.getByRole("button", "Retry", { exact: true }).tap();
    await expect(count).toHaveText("2");
    await expect(screen.getByText("Linked threads on", { exact: false })).toHaveCount(0);
  },
);
