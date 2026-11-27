import { expect } from "e2e";

import { answerIssueListPages } from "./issue-list-pages.ts";
import { test, WEB } from "./t3.ts";

test(
  "A failed Issue page keeps the loaded rows, and Retry reads on",
  WEB,
  async ({ t3, app, browser, screen }) => {
    await t3.project("demo");
    // Page two fails as a read, then GitHub refuses it inside an answered read, then it arrives.
    await answerIssueListPages(browser, {
      first: [{ issues: [3, 2], next: "page-2" }],
      "page-2": [
        { issues: [], failure: "rpc" },
        { issues: [], failure: "github" },
        { issues: [1] },
      ],
    });
    const retry = screen.getByRole("button", "Retry", { exact: true });
    const total = (text: string) => screen.getByText(text, { exact: true });
    // The total can only grow while a page is missing; a group count can also fall.
    const group = browser.locator("button[aria-expanded] .tabular-nums");

    await t3.open("/pull-requests?view=issues");
    await screen
      .getByText("Showing the last Issues loaded.", { exact: false })
      .waitFor({ timeout: 60_000 });
    await expect(screen.getByText("Open Issue 3", { exact: true })).toBeVisible();
    await expect(total("2+ open Issues")).toBeVisible();
    await expect(group).toHaveText("~2");
    await app.screenshot("failed-read");

    await retry.tap();
    await screen.getByText("GitHub API rate limit exceeded.", { exact: false }).waitFor();
    await expect(screen.getByText("Open Issue 3", { exact: true })).toBeVisible();
    await expect(total("2+ open Issues")).toBeVisible();
    await app.screenshot("github-refusal");

    await retry.tap();
    await expect(total("3 open Issues")).toBeVisible();
    await expect(group).toHaveText("3");
    await expect(screen.getByText("Open Issue 1", { exact: true })).toBeVisible();
  },
);
