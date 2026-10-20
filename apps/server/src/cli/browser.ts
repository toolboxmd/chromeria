import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { Command, Flag } from "effect/cli";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as PreviewBrowser from "../preview/PreviewBrowser.ts";
import * as PreviewBrowserHost from "../preview/PreviewBrowserHost.ts";
import { resolveBaseDir } from "../os-jank.ts";
import { baseDirFlag } from "./config.ts";
import { resolveRootCliCommand } from "./invocation.ts";
import * as BrowserRenewalCommitments from "../auth/BrowserRenewalCommitments.ts";

export class BrowserSetupStepError extends Schema.TaggedError<BrowserSetupStepError>()(
  "BrowserSetupStepError",
  { step: Schema.String, detail: Schema.String },
) {
  override get message(): string {
    return `Could not ${this.step}: ${this.detail}`;
  }
}

const runStep = Effect.fn("browserSetup.runStep")(function* (
  step: string,
  command: string,
  args: ReadonlyArray<string>,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const exitCode = yield* spawner
    .exitCode(
      ChildProcess.make(command, args, { stdin: "inherit", stdout: "inherit", stderr: "inherit" }),
    )
    .pipe(Effect.mapError((cause) => new BrowserSetupStepError({ step, detail: String(cause) })));
  if (exitCode !== 0) {
    return yield* new BrowserSetupStepError({ step, detail: `${command} exited with ${exitCode}` });
  }
});

/**
 * Under `sudo` the process home is root's, so an unspecified home falls back to
 * the invoking user's `~/.t3`.
 */
const setupBaseDir = Effect.fn("browserSetup.baseDir")(function* (explicit: Option.Option<string>) {
  const env = yield* HostProcess.Environment;
  const raw = Option.getOrUndefined(explicit) ?? env.T3CODE_HOME;
  if (raw !== undefined || env.SUDO_USER === undefined) return yield* resolveBaseDir(raw);
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const entry = yield* spawner
    .string(
      ChildProcess.make("getent", ["passwd", env.SUDO_USER], { stdin: "ignore", stderr: "ignore" }),
    )
    .pipe(Effect.orElseSucceed(() => ""));
  const home = entry.trim().split(":")[5];
  const path = yield* Path.Path;
  return home ? path.join(home, ".t3") : yield* resolveBaseDir(undefined);
});

const aptOffers = Effect.fn("browserSetup.aptOffers")(function* (name: string) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const policy = yield* spawner
    .string(ChildProcess.make("apt-cache", ["policy", name], { stdin: "ignore", stderr: "ignore" }))
    .pipe(Effect.orElseSucceed(() => ""));
  const candidate = /Candidate:\s*(\S+)/.exec(policy)?.[1];
  return candidate !== undefined && candidate !== "(none)";
});

const installedBrowser = Effect.fn("browserSetup.installedBrowser")(function* (baseDir: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const release = PreviewBrowser.previewBrowserRelease(
    yield* HostProcess.Platform,
    yield* HostProcess.Architecture,
  );
  if (release === null) return Option.none<string>();
  const executable = path.join(
    baseDir,
    "tools",
    PreviewBrowser.PREVIEW_BROWSER_DIRECTORY,
    release.platform,
    release.version,
    ...release.executable.split("/"),
  );
  return (yield* fs.exists(executable).pipe(Effect.orElseSucceed(() => false)))
    ? Option.some(executable)
    : Option.none<string>();
});

const browserSetupCommand = Command.make("setup", { baseDir: baseDirFlag }).pipe(
  Command.withDescription(
    "Set up this Linux host for T3's browser: allow Chrome's sandbox and install its libraries.",
  ),
  Command.withHandler(({ baseDir }) =>
    Effect.gen(function* () {
      if ((yield* HostProcess.Platform) !== "linux") {
        return yield* Console.log("Nothing to set up: T3's browser runs as is on this system.");
      }
      const fs = yield* FileSystem.FileSystem;
      const isRoot = (yield* HostProcess.UserId) === 0;
      const setupCommand = yield* resolveRootCliCommand(PreviewBrowserHost.SETUP_SUBCOMMAND);

      const needsProfile = yield* PreviewBrowserHost.sandboxBlocked;
      const browser = yield* installedBrowser(yield* setupBaseDir(baseDir));
      const missing = Option.isSome(browser)
        ? yield* PreviewBrowserHost.missingLibraries(browser.value)
        : [];
      const hasApt = yield* fs.exists("/usr/bin/apt-get").pipe(Effect.orElseSucceed(() => false));

      if (!needsProfile && missing.length === 0) {
        return yield* Console.log(
          Option.isSome(browser)
            ? "This host is ready for T3's browser."
            : "Chrome's sandbox is allowed here. T3's browser installs on first use; if it then reports missing libraries, run this again.",
        );
      }

      if (!isRoot) {
        if (needsProfile) {
          yield* Console.log(
            `This host blocks the sandbox T3's browser runs in. Setup installs an AppArmor profile at ${PreviewBrowserHost.APPARMOR_PROFILE_PATH} that allows it.`,
          );
        }
        if (missing.length > 0) {
          yield* Console.log(`T3's browser is missing ${missing.join(", ")}; setup installs them.`);
        }
        return yield* Console.log(`\nThis needs root. Run:\n\n  ${setupCommand}\n`);
      }

      if (needsProfile) {
        yield* fs
          .writeFileString(
            PreviewBrowserHost.APPARMOR_PROFILE_PATH,
            PreviewBrowserHost.APPARMOR_PROFILE,
          )
          .pipe(
            Effect.mapError(
              (cause) =>
                new BrowserSetupStepError({
                  step: `write ${PreviewBrowserHost.APPARMOR_PROFILE_PATH}`,
                  detail: cause.message,
                }),
            ),
          );
        yield* runStep("load the AppArmor profile", "apparmor_parser", [
          "-r",
          PreviewBrowserHost.APPARMOR_PROFILE_PATH,
        ]);
        yield* Console.log("Allowed Chrome's sandbox for T3's browser.");
      }

      if (missing.length > 0) {
        if (!hasApt) {
          return yield* Console.log(
            `T3's browser is missing ${missing.join(", ")}. Install them with your package manager, then run this again.`,
          );
        }
        yield* runStep("refresh the package lists", "apt-get", ["update"]);
        const packages = yield* Effect.forEach(PreviewBrowserHost.DEBIAN_PACKAGES, (names) =>
          Effect.findFirst(names, aptOffers).pipe(Effect.map(Option.getOrElse(() => names[0]!))),
        );
        yield* runStep("install the browser's libraries", "apt-get", [
          "install",
          "-y",
          "--no-install-recommends",
          ...packages,
        ]);
        yield* Console.log("Installed the browser's libraries.");
      }

      yield* Console.log("This host is ready for T3's browser.");
    }),
  ),
);

const browserCommitmentsCommand = Command.make("renewal-commitments", {
  executable: Flag.String("browser-executable"),
  profileDirectory: Flag.String("profile-dir"),
  origin: Flag.String("origin"),
  environmentId: Flag.String("environment-id").pipe(Flag.optional),
  json: Flag.Boolean("json").pipe(Flag.withDefault(false)),
}).pipe(
  Command.withDescription(
    "Read public renewal commitments from a private copy of the paired Chromium profile.",
  ),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      const rows = yield* BrowserRenewalCommitments.read(flags);
      const environmentId = Option.getOrUndefined(flags.environmentId);
      const filtered =
        environmentId === undefined
          ? rows
          : rows.filter((row) => row.environmentId === environmentId);
      yield* Console.log(
        flags.json
          ? yield* Schema.encodeEffect(
              Schema.fromJsonString(BrowserRenewalCommitments.PublicCommitments),
            )(filtered)
          : filtered
              .map((row) => `${row.environmentId} ${row.origin}\n  ${row.commitment}`)
              .join("\n"),
      );
    }).pipe(Effect.provide(BrowserRenewalCommitments.layer), Effect.scoped),
  ),
);

export const browserCommand = Command.make("browser").pipe(
  Command.withDescription("Manage T3's headless browser on this host."),
  Command.withSubcommands([browserSetupCommand, browserCommitmentsCommand]),
);
