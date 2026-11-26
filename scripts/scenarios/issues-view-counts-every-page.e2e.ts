import { expect } from "e2e";

import { answerIssueListPages } from "./issue-list-pages.ts";
import { test, WEB } from "./t3.ts";

test(
  "The Issues view counts the Issues of every page without a click",
  WEB,
  async ({ t3, app, browser, screen }) => {
    await t3.project("demo");
    // Three open Issues, of which GitHub's first page holds two.
    await answerIssueListPages(browser, {
      first: [{ issues: [3, 2], next: "page-2" }],
      "page-2": [{ issues: [1] }],
    });

    await t3.open("/pull-requests?view=issues");
    await screen.getByText("Open Issue 3", { exact: true }).waitFor({ timeout: 60_000 });
    try {
      // None of them has a thread or a pull request, so To do is the only group.
      await expect(browser.locator("button[aria-expanded] .tabular-nums")).toHaveText("3");
    } finally {
      await app.screenshot("to-do-count");
    }
  },
);
