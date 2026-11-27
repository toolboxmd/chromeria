import { expect } from "e2e";
import * as Schema from "effect/Schema";

import { SECOND_HOME, pairSecondServer, startSecondServer } from "./second-server.ts";
import { makeState, runPromise, test, WEB } from "./t3.ts";

const decodePairing = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Struct({ credential: Schema.String })),
);

test(
  "Removing a connection clears its permission update notice",
  WEB,
  async ({ t3, app, screen }) => {
    await t3.project("demo");
    const server = startSecondServer(app.baseUrl ?? "");
    try {
      const { host } = await server.ready;
      const { cli } = makeState(SECOND_HOME);
      const issued = await runPromise(
        cli(["auth", "pairing", "create", "--scope", "orchestration:read", "--json"]),
      );
      const { credential } = await runPromise(decodePairing(issued.stdout));

      await t3.open("/settings/connections");
      const actions = await pairSecondServer(screen, { host, token: credential });

      const notice = screen.getByText("Permissions have changed for Scenario", { exact: true });
      await notice.waitFor({ timeout: 60_000 });
      await actions.press("Enter");
      await screen.getByRole("menuitem", "Remove from this device…").press("Enter");
      await screen.getByRole("button", "Confirm").press("Enter");
      await actions.waitFor({ state: "hidden" });

      await app.screenshot("after-removing-connection");
      await expect(notice).toBeHidden();
    } finally {
      await server.stop();
    }
  },
);
