import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { prepareGrokBare, grokBareHistoryEvidence } from "./GrokBareRuntime.ts";

it.layer(NodeServices.layer)("Grok bare native inventory", (it) => {
  it.effect.each(["stable", "enabled-hook", "changing", "unknown"] as const)(
    "%s inventory is checked before a session can be made",
    (scenario) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped();
        yield* fs.makeDirectory(`${root}/native`);
        yield* fs.writeFileString(`${root}/native/auth.json`, "FAKE_TEST_AUTH");
        let inspections = 0;
        const spawner = ChildProcessSpawner.make((command) =>
          Effect.gen(function* () {
            expect(command._tag).toBe("StandardCommand");
            if (command._tag !== "StandardCommand") return yield* Effect.die("Unexpected pipe");
            expect(command.args).toEqual(["inspect", "--json"]);
            expect(command.options.cwd).toBe(`${root}/destination`);
            expect(command.options.env).toMatchObject({
              GROK_HOME: `${root}/bare`,
              HOME: root,
              GROK_MEMORY: "0",
            });
            const config = yield* fs.readFileString(`${root}/bare/config.toml`);
            if (inspections > 0) {
              expect(config).toContain('"skill-0"');
              expect(config).toContain('disabled = ["plugin"]');
            }
            const payload =
              scenario === "unknown"
                ? {}
                : {
                    skills: [
                      {
                        name: `skill-${scenario === "changing" ? inspections : 0}`,
                        disabled: inspections > 0,
                      },
                    ],
                    // Native inspect retains enabled=true even with an effective plugin disable.
                    plugins: [{ name: "plugin", enabled: true }],
                    hooks: [
                      {
                        event: "PreToolUse",
                        source:
                          scenario === "enabled-hook"
                            ? { type: "user" }
                            : { type: "plugin", plugin_name: "plugin" },
                      },
                    ],
                  };
            inspections++;
            return ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(1),
              exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
              isRunning: Effect.succeed(false),
              kill: () => Effect.void,
              unref: Effect.succeed(Effect.void),
              stdin: Sink.drain,
              stdout: Stream.encodeText(Stream.make(JSON.stringify(payload))),
              stderr: Stream.empty,
              all: Stream.empty,
              getInputFd: () => Sink.drain,
              getOutputFd: () => Stream.empty,
            });
          }),
        );
        const prepared = yield* prepareGrokBare({
          home: `${root}/bare`,
          cwd: `${root}/destination`,
          environment: { HOME: root, GROK_HOME: `${root}/native` },
          binary: "grok",
        }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.result,
        );
        if (scenario === "stable") {
          expect(prepared._tag).toBe("Success");
          if (prepared._tag !== "Success") return;
          expect(inspections).toBe(2);
          expect(prepared.success.skills).toEqual(["skill-0"]);
          expect(prepared.success.plugins).toEqual(["plugin"]);
          expect(prepared.success.observedDisabledSkills).toEqual(["skill-0"]);
          expect(yield* fs.readLink(`${root}/bare/auth.json`)).toBe(`${root}/native/auth.json`);
          expect(yield* fs.readLink(`${root}/bare/auth.json.lock`)).toBe(
            `${root}/native/auth.json.lock`,
          );
        } else {
          expect(prepared._tag).toBe("Failure");
          if (prepared._tag !== "Failure") return;
          expect(prepared.failure.message).toContain(
            scenario === "enabled-hook"
              ? "enabled user hook"
              : scenario === "changing"
                ? "three passes"
                : "missing required identities",
          );
          expect(inspections).toBe(
            scenario === "changing" ? 3 : scenario === "enabled-hook" ? 2 : 1,
          );
        }
      }),
  );
  it.effect(
    "native history marks a skill reminder unexpected even when prompt_context has no reminder",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped();
        const sessionId = "native-session";
        const cwd = "/destination";
        const directory = `${root}/sessions/${encodeURIComponent(cwd)}/${sessionId}`;
        yield* fs.makeDirectory(directory, { recursive: true });
        yield* fs.writeFileString(
          `${directory}/prompt_context.json`,
          JSON.stringify({ system: "." }),
        );
        yield* fs.writeFileString(
          `${directory}/chat_history.jsonl`,
          JSON.stringify({
            type: "system_reminder",
            content: "<skills_reminder>Unexpected skill</skills_reminder>",
          }) + "\n",
        );
        const evidence = yield* grokBareHistoryEvidence(root, cwd, sessionId);
        expect(evidence).toContainEqual(expect.objectContaining({ status: "observed_unexpected" }));
        expect(JSON.stringify(evidence)).not.toContain("Unexpected skill");
      }),
  );
});
