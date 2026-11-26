import { expect } from "e2e";

import { answerIssueListPages } from "./issue-list-pages.ts";
import { test, WEB } from "./t3.ts";

test(
  "The Issues view states the week's flow and the stale Issues, and lists without the flow when it cannot be counted",
  WEB,
  async ({ t3, app, browser, screen }) => {
    await t3.project("demo");
    // The first read comes without the week's counts, as from a server whose count failed.
    await answerIssueListPages(
      browser,
      {
        first: [
          { issues: [3, 2, 1], week: null },
          { issues: [3, 2, 1], week: { filed: 5, closed: 2 } },
        ],
      },
      { facts: { 3: { fresh: true } } },
    );
    const says = (text: string) => screen.getByText(text, { exact: true });

    await t3.open("/pull-requests?view=issues");
    await says("Open Issue 3").waitFor({ timeout: 60_000 });
    await expect(says("3 open Issues")).toBeVisible();
    await expect(says("Last 7 days: not counted")).toBeVisible();
    await expect(says("Not updated for 30 days: 2")).toBeVisible();
    await app.screenshot("not-counted");

    await screen.getByRole("button", "Refresh Issues", { exact: true }).tap();
    try {
      await expect(says("Last 7 days: 5 filed, 2 closed")).toBeVisible();
      await expect(says("3 open Issues")).toBeVisible();
    } finally {
      await app.screenshot("numbers");
    }
  },
);
