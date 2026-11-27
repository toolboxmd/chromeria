/// <reference lib="dom" />
import { expect } from "e2e";

import { answerIssueListPages } from "./issue-list-pages.ts";
import { test, WEB } from "./t3.ts";

test(
  "A narrow, indented Issue row keeps its link whole and in reach of a click and the keyboard",
  WEB,
  async ({ t3, app, browser, screen }) => {
    const project = await t3.project("demo");
    const idle = await t3.thread({ project, title: "Finished its turn", messages: ["Start"] });
    // Each Issue is a sub-Issue of the one before, so the parent tree indents the last two as far
    // as it goes. One names a pull request, the other a thread.
    await answerIssueListPages(
      browser,
      { first: [{ issues: [7, 6, 5, 4, 3, 2, 1] }] },
      {
        facts: {
          6: { parent: 7 },
          5: { parent: 6 },
          4: { parent: 5 },
          3: { parent: 4 },
          2: { parent: 3, pullRequest: { number: 12, review: "success", checks: "failure" } },
          1: { parent: 2, threads: [idle.threadId] },
        },
      },
    );
    await browser.addInitScript(() =>
      localStorage.setItem("t3code:issue-list-preferences", '{"groupByParent":true}'),
    );
    /** How the link named `name` sits in its row: cut by its line, covered, or neither. */
    const placed = (name: string) =>
      browser.evaluate((name) => {
        const link = document.querySelector<HTMLElement>(`a[aria-label="${name}"]`);
        if (link === null) return null;
        const line = link.parentElement!.getBoundingClientRect();
        const box = link.getBoundingClientRect();
        const top = document.elementFromPoint(box.left + box.width / 2, box.top + box.height / 2);
        return {
          rowWidth: Math.round(link.closest(".group\\/issue-row")!.getBoundingClientRect().width),
          cut: box.width === 0 || box.left < line.left - 0.5 || box.right > line.right + 0.5,
          onTop: top !== null && link.contains(top),
        };
      }, name);

    // The longest of the link's translations.
    await t3.open("/settings/general");
    await screen.getByLabel("Language", { exact: true }).tap();
    await screen.getByText("Français", { exact: true }).tap();
    await screen.getByLabel("Langue", { exact: true }).waitFor();
    await browser.setViewport({ width: 720, height: 800 });
    await t3.open("/pull-requests?view=issues");
    await screen.getByText("Open Issue 1", { exact: true }).waitFor({ timeout: 60_000 });
    try {
      for (const name of ["Ouvrir la pull request #12", "Ouvrir le fil"]) {
        const link = await placed(name);
        expect(link, `The row has no link named ${name}`).not.toBeNull();
        // Narrower than a row beside the open side panel at 900 px.
        expect(link!.rowWidth).toBeLessThan(384);
        expect(link, `${name} is cut or covered`).toMatchObject({ cut: false, onTop: true });
      }
    } finally {
      await app.screenshot("narrow-indented-rows");
    }

    // From the row's title, the next Tab stop is its link, and the link opens the thread.
    await browser.evaluate(() => {
      document
        .querySelector('a[aria-label="Ouvrir le fil"]')
        ?.closest(".group\\/issue-row")
        ?.querySelector("button")
        ?.focus();
      return null;
    });
    await browser.keyboard.press("Tab");
    expect(
      await browser.evaluate(() => document.activeElement?.getAttribute("aria-label") ?? null),
    ).toBe("Ouvrir le fil");
    await browser.keyboard.press("Enter");
    await expect(browser).toHaveURL(new RegExp(`${idle.threadId.split(":").at(-1)}$`));
  },
);
