import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { writeFakeCli } from "@t3tools/provider-testing/fakeCli";
import {
  prepareGrokBare,
  grokBareHistoryEvidence,
  grokBareSessionMeta,
} from "./GrokBareRuntime.ts";
import { makeGrokAcpRuntime } from "./acpSupport.ts";

const decodeNativeRequestLog = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));

it.layer(NodeServices.layer)("Grok native session metadata", (it) => {
  it.effect.each([false, true])("sends foreground tools only for bare sessions, bare=%s", (bare) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.makeTempDirectoryScoped();
      const logPath = `${root}/requests.json`;
      const binary = writeFakeCli({
        directory: root,
        name: "grok-metadata-test",
        source: `
          import { writeFileSync } from "node:fs";
          import { createInterface } from "node:readline";
          const requests = [];
          for await (const line of createInterface({ input: process.stdin })) {
            const request = JSON.parse(line);
            requests.push(request);
            writeFileSync(process.env.GROK_TEST_REQUEST_LOG, JSON.stringify({
              args: process.argv.slice(2), requests,
            }));
            const result = request.method === "initialize"
              ? { protocolVersion: request.params.protocolVersion, agentCapabilities: {} }
              : request.method === "session/new" ? { sessionId: "metadata-session" } : null;
            if (result === null) throw new Error("Unexpected method: " + request.method);
            process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id: request.id, result }) + "\\n");
          }
        `,
      });
      const runtime = yield* makeGrokAcpRuntime({
        grokSettings: { binaryPath: binary },
        cwd: root,
        clientInfo: { name: "grok-metadata-test", version: "1" },
        environment: { GROK_TEST_REQUEST_LOG: logPath },
        runtimeMode: "full-access",
        ...(bare ? { bare: true, sessionMeta: grokBareSessionMeta(true) } : {}),
      });
      yield* runtime.start();
      const wire = yield* decodeNativeRequestLog(yield* fs.readFileString(logPath));
      expect(wire).toEqual({
        args: bare
          ? ["agent", "--always-approve", "--no-leader", "stdio"]
          : ["agent", "--always-approve", "stdio"],
        requests: [
          expect.objectContaining({
            method: "initialize",
            params: expect.objectContaining({ _meta: { clientType: "extension" } }),
          }),
          expect.objectContaining({
            method: "session/new",
            params: {
              cwd: root,
              mcpServers: [],
              ...(bare
                ? {
                    _meta: {
                      systemPromptOverride: ".",
                      yoloMode: true,
                      agentProfile: {
                        name: "bare",
                        description: "Bare launch",
                        tools: [
                          "run_terminal_command",
                          "read_file",
                          "search_replace",
                          "list_dir",
                          "grep",
                          "write",
                        ],
                        disallowedTools: [
                          "search_tool",
                          "use_tool",
                          "todo_write",
                          "monitor",
                          "update_goal",
                          "web_search",
                          "web_fetch",
                          "image_gen",
                          "image_edit",
                          "image_to_video",
                          "reference_to_video",
                          "enter_plan_mode",
                          "exit_plan_mode",
                          "ask_user_question",
                        ],
                      },
                    },
                  }
                : {}),
            },
          }),
        ],
      });
    }).pipe(Effect.provideService(HostProcess.Platform, "darwin")),
  );
});

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
