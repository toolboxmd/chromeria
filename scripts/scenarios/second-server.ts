// A second T3 server for scenarios that need another machine: its own T3 home, the replay
// agent, and a pairing token for Settings > Connections > Add environment.
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { waitForHttpReady } from "@t3tools/shared/httpReadiness";
import * as NetService from "@t3tools/shared/Net";
import type { TestFixtures } from "e2e";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Path from "effect/Path";
import { FetchHttpClient } from "effect/http";

import { APP_DIR, T3_HOME } from "../e2e.config.ts";
import { ScenarioError } from "./shell.ts";
import { prepareHome, SCENARIO_MACHINE_ENV, start } from "./start-app.ts";

export const SECOND_HOME = `${T3_HOME}-second-environment`;

export async function pairSecondServer(
  screen: TestFixtures["screen"],
  { host, token }: { readonly host: string; readonly token: string },
) {
  const add = screen.getByRole("button", "Add environment", { exact: true });
  await add.waitFor({ timeout: 60_000 });
  await add.tap();
  const dialog = screen.getByRole("dialog", "Add Environment", { exact: true });
  await dialog.getByPlaceholder("backend.example.com").fill(host);
  await dialog.getByPlaceholder("PAIRCODE").fill(token);
  await dialog.getByRole("button", "Add environment", { exact: true }).tap();
  await dialog.waitFor({ state: "hidden" });
  await screen.getByRole("button", "List", { exact: true }).tap();
  const actions = screen.getByRole("button", "More actions for Scenario", { exact: true });
  await actions.waitFor({ timeout: 60_000 });
  return actions;
}

/** A second server on its own T3 home, running until interrupted. `--dev-url` admits the web
 * origin through CORS and registers the replay agent; `paired` gets the Add environment input. */
const runSecondServer = Effect.fn("scenario.secondServer")(function* (
  devUrl: string,
  paired: (target: { readonly host: string; readonly token: string }) => void,
) {
  const path = yield* Path.Path;
  const node = yield* HostProcess.ExecutablePath;
  const hostEnvironment = yield* HostProcess.Environment;
  const port = yield* (yield* NetService.NetService).reserveLoopbackPort();
  yield* prepareHome(SECOND_HOME);
  const token = yield* Deferred.make<string>();
  const server = yield* start({
    cwd: APP_DIR,
    command: node,
    args: [
      "apps/server/src/bin.ts",
      "serve",
      "--host",
      "127.0.0.1",
      "--port",
      String(port),
      "--base-dir",
      SECOND_HOME,
      "--dev-url",
      devUrl,
    ],
    env: {
      PATH: `${path.join(SECOND_HOME, "bin")}:${hostEnvironment.PATH ?? ""}`,
      HOME: hostEnvironment.HOME,
      T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "0",
      ...SCENARIO_MACHINE_ENV,
    },
    onLine: (line) => {
      const value = /^Token:\s*(\S+)/.exec(line)?.[1];
      return value === undefined ? Effect.void : Deferred.succeed(token, value).pipe(Effect.asVoid);
    },
  });
  // The CLI can print the token before the environment endpoint is ready.
  const exited = server.exitCode.pipe(
    Effect.flatMap(
      (code) =>
        new ScenarioError({ message: `The second server exited with code ${code} at startup.` }),
    ),
  );
  const host = `http://127.0.0.1:${port}`;
  const ready = Effect.gen(function* () {
    const credential = yield* Deferred.await(token);
    yield* waitForHttpReady({
      baseUrl: host,
      path: "/.well-known/t3/environment",
      makeError: ({ requestUrl }) =>
        new ScenarioError({ message: `The second server did not become ready at ${requestUrl}.` }),
    });
    return { host, token: credential };
  });
  paired(yield* Effect.raceFirst(ready, exited));
  return yield* Effect.never;
});

/** Starts the second server for the web app at `devUrl`. `ready` settles with its pairing input. */
export function startSecondServer(devUrl: string) {
  const pairing = Promise.withResolvers<{ readonly host: string; readonly token: string }>();
  const server = Effect.runFork(
    runSecondServer(devUrl, pairing.resolve).pipe(
      Effect.scoped,
      Effect.provide([NodeServices.layer, NetService.layer, FetchHttpClient.layer]),
    ),
  );
  server.addObserver((exit) =>
    pairing.reject(
      Exit.isFailure(exit) ? Cause.squash(exit.cause) : new Error("The second server stopped."),
    ),
  );
  return { ready: pairing.promise, stop: () => Effect.runPromise(Fiber.interrupt(server)) };
}
