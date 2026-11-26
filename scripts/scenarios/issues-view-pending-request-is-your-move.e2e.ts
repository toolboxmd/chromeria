import { expect } from "e2e";

import { answerIssueListPages } from "./issue-list-pages.ts";
import { test, WEB } from "./t3.ts";

test(
  "An Issue whose linked thread asks the user a question shows under Your move and opens that thread",
  WEB,
  async ({ t3, app, browser, screen }) => {
    const project = await t3.project("demo");
    // One thread stops on a question to the user; the other finished its turn.
    const asking = await t3.thread({
      project,
      title: "Asks before going on",
      messages: ["Start"],
      model: "question",
    });
    const idle = await t3.thread({ project, title: "Finished its turn", messages: ["Start"] });
    await answerIssueListPages(
      browser,
      { first: [{ issues: [2, 1] }] },
      { facts: { 2: { threads: [asking.threadId] }, 1: { threads: [idle.threadId] } } },
    );
    const header = (place: number) =>
      browser.locator(`ul > li:nth-child(${place}) > button[aria-expanded]`);

    await t3.open("/pull-requests?view=issues");
    await screen.getByText("Open Issue 2", { exact: true }).waitFor({ timeout: 60_000 });
    try {
      // No GitHub read says a thread waits: the pending question comes from the thread itself.
      await expect(header(1)).toContainText("Your move");
      await expect(
        screen.getByText("A thread is waiting for your answer", { exact: true }),
      ).toBeVisible();
      await expect(header(2)).toContainText("Stalled");
    } finally {
      await app.screenshot("your-move");
    }

    // The row of the Issue that waits is first, so its action is the first of the two.
    await screen.getByRole("button", "Open the thread", { exact: true }).nth(0).tap();
    await expect(browser).toHaveURL(new RegExp(`${asking.threadId.split(":").at(-1)}$`));
  },
);
