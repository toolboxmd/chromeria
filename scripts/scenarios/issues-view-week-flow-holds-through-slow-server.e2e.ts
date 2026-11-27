import { expect } from "e2e";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";

import { answerIssueListPages, releaseIssueReads } from "./issue-list-pages.ts";
import { SECOND_HOME, startSecondServer } from "./second-server.ts";
import { environmentId, makeState, runPromise, test, WEB } from "./t3.ts";

test(
  "The Issues view keeps its open total and week's flow while a slower server reads the rows again",
  WEB,
  async ({ t3, app, browser, screen }) => {
    await t3.project("demo");
    const server = startSecondServer(app.baseUrl ?? "");
    try {
      const { host, token } = await server.ready;
      await makeState(SECOND_HOME).state.project("remote-demo");
      // The server prints its token before it listens, and a pairing refused then leaves the
      // client backing off for longer than the scenario waits.
      await runPromise(
        environmentId(host).pipe(
          Effect.retry({ schedule: Schedule.spaced("200 millis"), times: 150 }),
        ),
      );
      // Both servers list the one repository, and a search matches one Issue. The second
      // server's answers are held back, the listing and the search alike.
      await answerIssueListPages(
        browser,
        { first: [{ issues: [3, 2, 1] }, { issues: [2], matches: 1 }] },
        {
          counts: [{ open: 3, filed: 4, closed: 2 }],
          second: {
            host: new URL(host).host,
            plan: {
              first: [
                { issues: [3, 2, 1], held: true },
                { issues: [2], matches: 1, held: true },
              ],
            },
          },
        },
      );
      const says = (text: string) => screen.getByText(text, { exact: true });

      await t3.open("/settings/connections");
      await screen.getByRole("button", "Add environment").tap();
      const dialog = screen.getByRole("dialog");
      await dialog.getByPlaceholder("backend.example.com").fill(host);
      await dialog.getByPlaceholder("PAIRCODE").fill(token);
      await dialog.getByRole("button", "Add environment").tap();
      await dialog.getByPlaceholder("PAIRCODE").waitFor({ state: "hidden", timeout: 60_000 });
      await app.open("/pull-requests?view=issues");
      await says("Open Issue 3").waitFor({ timeout: 60_000 });
      // Until the second server's listing answers, which repositories to count is not known.
      await says("Last 7 days: unknown").waitFor({ timeout: 60_000 });
      await releaseIssueReads(browser);
      await expect(says("Last 7 days: 4 filed, 2 closed")).toBeVisible();
      await expect(says("3 open Issues")).toBeVisible();

      await screen.getByRole("searchbox", "Search Issues", { exact: true }).fill("two");
      // The first server's rows are in; the second server has not answered the search.
      await says("Open Issue 3").waitFor({ state: "hidden" });
      await says("Open Issue 2").waitFor();
      try {
        await expect(says("Last 7 days: 4 filed, 2 closed")).toBeVisible();
        await expect(says("1+ of 3 open Issues")).toBeVisible();
      } finally {
        await app.screenshot("second-server-pending");
      }
      await releaseIssueReads(browser);
      await expect(says("1 of 3 open Issues")).toBeVisible();
      await expect(says("Last 7 days: 4 filed, 2 closed")).toBeVisible();
    } finally {
      await server.stop();
    }
  },
);
