import { expect } from "e2e";

import { answerIssueListPages } from "./issue-list-pages.ts";
import { test, WEB } from "./t3.ts";

test(
  "The Issues view groups open Issues by whose move it is, and each row says why",
  WEB,
  async ({ t3, app, browser, screen }) => {
    await t3.project("demo");
    await answerIssueListPages(
      browser,
      { first: [{ issues: [9, 8, 7, 6, 5, 4, 3, 2, 1] }] },
      {
        facts: {
          9: { labels: ["needs-user"] },
          // Reviewed with its checks done, reviewed with its checks running, and a reviewed draft.
          8: { pullRequest: { number: 18, review: "success", checks: "failure" } },
          7: { pullRequest: { number: 17, review: "success", checks: "pending" } },
          6: { labels: ["waiting"], pullRequest: { number: 16, review: "pending" } },
          5: { openBlockerCount: 2 },
          4: { pullRequest: { number: 14 } },
          3: { pullRequest: { number: 13, isDraft: true, review: "success", checks: "success" } },
        },
      },
    );
    /** A group's header by its place in the list: its name and its count. */
    const group = (place: number) => `ul > li:nth-child(${place}) > button[aria-expanded]`;
    const says = (text: string) => screen.getByText(text, { exact: true });

    await t3.open("/pull-requests?view=issues");
    await says("Open Issue 9").waitFor({ timeout: 60_000 });
    try {
      for (const [place, name, count] of [
        [1, "Your move", "2"],
        [2, "Agents working", "1"],
        [3, "Waiting", "1"],
        [4, "Blocked", "1"],
        [5, "Stalled", "2"],
        [6, "Nobody on it", "2"],
      ] as const) {
        await expect(browser.locator(group(place))).toContainText(name);
        await expect(browser.locator(`${group(place)} .tabular-nums`)).toHaveText(count);
      }
      await expect(says("9 open Issues")).toBeVisible();
      // Each row outside Nobody on it says why it is where it is.
      for (const reason of [
        "Needs you",
        "Reviewed, not merged",
        "Reviewed, checks running",
        "Blocked by 2 open Issues",
        "Waiting for review",
      ]) {
        await expect(says(reason)).toBeVisible();
      }
      // The pull request a reason names opens from the row; a label names none.
      await expect(screen.getByRole("link", "Open pull request #18")).toBeVisible();
      await expect(screen.getByRole("link", "Open pull request #16")).toHaveCount(0);
    } finally {
      await app.screenshot("groups");
    }
  },
);
