import { expect, it } from "@effect/vitest";
import { DeviceCommand, type DeviceCommand as Command } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as ProcessRunner from "../processRunner.ts";
import * as Commands from "./AgentDeviceCommand.ts";

const decodeCommandExit = Schema.decodeUnknownExit(DeviceCommand);

const fixture = (data: unknown = {}) => {
  const calls: ProcessRunner.ProcessRunInput[] = [];
  const runner = ProcessRunner.ProcessRunner.of({
    run: (call) =>
      Effect.sync(() => {
        calls.push(call);
        return {
          code: ChildProcessSpawner.ExitCode(0),
          stdout: JSON.stringify({ success: true, data }),
          stderr: "",
          timedOut: false,
          stdoutTruncated: false,
          stderrTruncated: false,
          stdoutInvalidUtf8: false,
          stderrInvalidUtf8: false,
        };
      }),
  });
  const run = (command: Exclude<Command, { type: "screenshot" | "batch" }>) =>
    Commands.run({
      command,
      runner,
      entryPath: "/private/installed/agent-device.mjs",
      nodePath: "/local/node",
      configPath: "/private/session.json",
      cwd: "/private",
      session: "private-session",
      platform: "android",
      deviceId: "emulator-383",
    });
  return { run, calls };
};

it.effect(
  "text that resembles flags stays data, while endpoint, config and runtime stay private",
  () =>
    Effect.gen(function* () {
      const f = fixture();
      yield* f
        .run({
          type: "type",
          text: "--session rival --config /borrowed --daemon-auth-token SECRET",
        })
        .pipe(
          Effect.provideService(HostProcess.Environment, {
            PATH: "/allowed",
            NODE_OPTIONS: "--import /unsafe",
            AGENT_DEVICE_CONFIG: "/borrowed",
            AGENT_DEVICE_DAEMON_AUTH_TOKEN: "BORROWED",
            OPENAI_API_KEY: "PRIVATE_PROVIDER_KEY",
            SSH_AUTH_SOCK: "/private/agent.sock",
            HOME: "/live/home",
          }),
        );
      const call = f.calls[0];
      expect(call?.command).toBe("/local/node");
      expect(call?.args).toEqual([
        "/private/installed/agent-device.mjs",
        "type",
        "--json",
        "--platform",
        "android",
        "--serial",
        "emulator-383",
        "--config",
        "/private/session.json",
        "--session",
        "private-session",
        "--",
        "--session rival --config /borrowed --daemon-auth-token SECRET",
      ]);
      expect(call?.env).toEqual({
        PATH: "/allowed",
        HOME: "/private",
        USERPROFILE: "/private",
        ELECTRON_RUN_AS_NODE: "1",
      });
      expect(call?.cwd).toBe("/private");
    }),
);

it.effect(
  "fill clears empty input and preserves CR and LF endings at the pinned stdin boundary",
  () =>
    Effect.gen(function* () {
      const f = fixture();
      for (const text of ["", "abc\r", "\n", " --config /rival\n\n"]) {
        yield* f.run({ type: "fill", ref: "@e12", text });
        const call = f.calls.at(-1);
        if (text === "") {
          expect(call?.stdin).toBeUndefined();
          expect(call?.args).not.toContain("--text-stdin");
          expect(call?.args.slice(-3)).toEqual(["--", "@e12", ""]);
        } else {
          expect(call?.stdin).toBe(text + "\r\n");
          expect(call?.args).toContain("--text-stdin");
          expect(call?.args.at(-1)).toBe("@e12");
          expect(call?.args).not.toContain(text);
        }
      }
    }),
);

it.effect("open retains a bounded app deep link as data after the private binding", () =>
  Effect.gen(function* () {
    const command = {
      type: "open",
      app: "com.example.app",
      url: "example://settings?tab=one",
    } as const;
    expect(decodeCommandExit(command)._tag).toBe("Success");
    const f = fixture();
    yield* f.run(command);
    expect(f.calls[0]?.args.slice(-3)).toEqual(["--", command.app, command.url]);
    for (const url of ["--config", "example://bad\nlink", "x:" + "a".repeat(8192)])
      expect(decodeCommandExit({ ...command, url })._tag).toBe("Failure");
  }),
);

it.effect(
  "snapshot emits only bounded public nodes; malformed or oversized results fail safely",
  () =>
    Effect.gen(function* () {
      const f = fixture({
        nodes: [
          {
            index: 1,
            ref: "e1",
            role: "button",
            label: "Continue",
            token: "PRIVATE",
            path: "/private",
          },
        ],
        token: "PRIVATE",
      });
      expect(yield* f.run({ type: "snapshot", interactiveOnly: true })).toEqual({
        nodes: [{ index: 1, ref: "e1", role: "button", label: "Continue" }],
      });
      for (const data of [
        { nodes: [{ index: 1, ref: "malformed" }] },
        { nodes: Array.from({ length: 2001 }, (_, index) => ({ index, ref: `e${index}` })) },
      ]) {
        const invalid = yield* fixture(data).run({ type: "snapshot" }).pipe(Effect.flip);
        expect(invalid.message).not.toMatch(/PRIVATE|\/private/);
      }
    }),
);

it("rejects forbidden fields and malformed platform-independent values at the public boundary", () => {
  const parse = decodeCommandExit;
  for (const input of [
    { type: "click", ref: "@e1", argv: ["--session", "rival"] },
    { type: "fill", ref: "@e1", text: "x", config: "/borrowed" },
    { type: "focus", x: Infinity, y: 1 },
    { type: "swipe", from: { x: 1, y: 2, serial: "rival" }, to: { x: 3, y: 4 } },
    { type: "wait", kind: "duration", durationMs: 60_001 },
    { type: "clipboard", action: "write" },
    { type: "open", app: "--daemon-base-url" },
  ])
    expect(parse(input)._tag).toBe("Failure");
});

it.effect(
  "gesture variants retain native ordering and defaults without accepting unrelated fields",
  () =>
    Effect.gen(function* () {
      const f = fixture();
      yield* f.run({
        type: "gesture",
        kind: "pan",
        origin: { x: 20, y: 30 },
        delta: { x: -5, y: 0 },
        pointerCount: 2,
      });
      expect(f.calls[0]?.args).toContain("--pointer-count");
      expect(f.calls[0]?.args.slice(f.calls[0].args.indexOf("--") + 1)).toEqual([
        "pan",
        "20",
        "30",
        "-5",
        "0",
      ]);
      yield* f.run({
        type: "gesture",
        kind: "drag",
        source: "@e1",
        destination: "@e2",
        destinationHoldMs: 200,
      });
      expect(f.calls[1]?.args.slice(f.calls[1].args.indexOf("--") + 1)).toEqual([
        "drag",
        "@e1",
        "@e2",
        "800",
        "500",
        "200",
      ]);
      for (const input of [
        { type: "gesture", kind: "pan", origin: { x: 1, y: 2 }, delta: { x: 3, y: 4 }, scale: 2 },
        { type: "gesture", kind: "drag", source: "@e1", destination: "@e2", sourceHoldMs: 10_000 },
        { type: "find", query: "Continue", action: "click", timeoutMs: 10 },
      ])
        expect(decodeCommandExit(input)._tag).toBe("Failure");
    }),
);

it.effect(
  "mobile settings and documented event payloads remain data under the private target binding",
  () =>
    Effect.gen(function* () {
      const f = fixture({
        status: "applied",
        priorGrantState: "granted",
        warning: "App was terminated",
      });
      const result = yield* f.run({
        type: "settings",
        setting: "permission",
        state: "deny",
        permission: "camera",
        app: "com.example.app",
      });
      expect(result).toEqual({
        document: { status: "applied", priorGrantState: "granted", warning: "App was terminated" },
      });
      expect(f.calls[0]?.args).toContain("--app");
      expect(f.calls[0]?.args.slice(f.calls[0].args.indexOf("--") + 1)).toEqual([
        "permission",
        "deny",
        "camera",
      ]);
      const payloadJson = '{"argv":["--config","/borrowed"],"event":"--session rival"}';
      yield* f.run({ type: "trigger-app-event", event: "test-seed", payloadJson });
      expect(f.calls[1]?.args.slice(f.calls[1].args.indexOf("--") + 1)).toEqual([
        "test-seed",
        payloadJson,
      ]);
      expect(
        decodeCommandExit({
          type: "settings",
          setting: "location",
          state: "set",
          latitude: 91,
          longitude: 0,
        })._tag,
      ).toBe("Failure");
      expect(
        decodeCommandExit({
          type: "trigger-app-event",
          event: "test",
          payloadJson: "not-json",
        })._tag,
      ).toBe("Failure");
      expect(decodeCommandExit({ type: "reinstall", artifact: {} })._tag).toBe("Failure");
    }),
);

it.effect(
  "snapshot preserves native occlusion, incomplete coverage and normalized accessibility facts",
  () =>
    Effect.gen(function* () {
      const data = {
        nodes: [
          {
            index: 1,
            ref: "e1",
            kind: "text-field",
            type: "android.widget.EditText",
            depth: 2,
            parentIndex: 0,
            identifier: "email",
            contentDescription: "Email",
            editable: true,
            rect: { x: 1, y: 2, width: 100, height: 30 },
            inheritsLabel: true,
            privatePath: "/host/private",
          },
        ],
        systemSurfaceOnly: true,
        comparisonSafe: false,
        viewport: { width: 400, height: 800 },
        keyboard: { kind: "unmeasurable", reason: "capture-path" },
        snapshotQuality: {
          state: "sparse",
          backend: "android-helper",
          reasonCode: "budget",
          effectiveDepth: 2,
          customActions: { read: 1, candidates: 2, truncated: 1, blocked: false },
        },
        postGestureOutcome: {
          kind: "unsettled",
          gesture: { action: "private", positionals: ["/private"] },
        },
        token: "PRIVATE",
      };
      const result = yield* fixture(data).run({
        type: "snapshot",
        customActions: true,
        observeOnly: true,
      });
      expect(result).toMatchObject({
        nodes: [
          {
            kind: "text-field",
            depth: 2,
            identifier: "email",
            editable: true,
            rect: { width: 100 },
            inheritsLabel: true,
          },
        ],
        systemSurfaceOnly: true,
        comparisonSafe: false,
        keyboard: data.keyboard,
        snapshotQuality: data.snapshotQuality,
        postGestureOutcome: { kind: "unsettled" },
      });
      expect(JSON.stringify(result)).not.toMatch(/PRIVATE|privatePath|positionals/);
    }),
);

it.effect("coordinate and selector variants stay literal under the same qualified target", () =>
  Effect.gen(function* () {
    const f = fixture();
    yield* f.run({ type: "click", element: { kind: "point", point: { x: 0, y: 400 } } });
    yield* f.run({
      type: "fill",
      element: { kind: "selector", selector: 'label="Email" editable=true' },
      text: "--session rival",
    });
    yield* f.run({
      type: "longpress",
      element: { kind: "point", point: { x: 10, y: 20 } },
      durationMs: 800,
    });
    yield* f.run({ type: "wait", kind: "selector", selector: "--config", timeoutMs: 100 });
    expect(f.calls.map((call) => call.args.slice(call.args.indexOf("--") + 1))).toEqual([
      ["0", "400"],
      ['label="Email" editable=true'],
      ["10", "20", "800"],
      ["--config", "100"],
    ]);
    expect(f.calls[1]?.stdin).toBe("--session rival\r\n");
    expect(
      decodeCommandExit({
        type: "click",
        ref: "@e1",
        element: { kind: "point", point: { x: 1, y: 2 } },
      })._tag,
    ).toBe("Failure");
    expect(
      decodeCommandExit({ type: "click", element: { kind: "point", point: { x: Infinity, y: 1 } } })
        ._tag,
    ).toBe("Failure");
  }),
);
