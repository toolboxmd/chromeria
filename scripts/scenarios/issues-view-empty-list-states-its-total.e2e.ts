import { expect } from "e2e";

import { answerIssueListPages } from "./issue-list-pages.ts";
import { test, WEB } from "./t3.ts";

test(
  "An Issues list read to its end with nothing in it states its total",
  WEB,
  async ({ t3, app, browser, screen }) => {
    await t3.project("demo");
    await answerIssueListPages(browser, { first: [{ issues: [] }] });

    await t3.open("/pull-requests?view=issues");
    await screen.getByText("No Issues match.", { exact: true }).waitFor({ timeout: 60_000 });
    try {
      await expect(screen.getByText("0 open Issues", { exact: true })).toBeVisible();
    } finally {
      await app.screenshot("empty-list");
    }
  },
);
