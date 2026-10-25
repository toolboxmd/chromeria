import * as HostProcess from "@t3tools/shared/HostProcess";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import type { BareReadGrant } from "./bareReadGrant.ts";
import * as BareRuntime from "@t3tools/provider-core/server/bareLaunch";
import { describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProviderInstanceId, ProviderDriverKind, type BareLaunchRequest } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as BareLaunch from "./BareLaunch.ts";
import * as Adapters from "./ProviderAdapterRegistry.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import * as Git from "../git/GitWorkflowService.ts";

const instanceId = ProviderInstanceId.make("codex");
const registry = Layer.mock(Adapters.ProviderAdapterRegistryV2)({
  get: () =>
    Effect.succeed({
      driver: "claudeAgent",
      bareSupport: Effect.succeed({ supported: true }),
    } as never),
});
const fixtures = (
  inventory: Providers.ProviderRegistry["Service"]["refreshWorkspaceSnapshot"] = () =>
    Effect.succeed([]),
) =>
  BareLaunch.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        registry,
        Layer.mock(Providers.ProviderRegistry)({ refreshWorkspaceSnapshot: inventory }),
        Layer.mock(Git.GitWorkflowService)({}),
      ),
    ),
    Layer.provide(NodeServices.layer),
  );

it.layer(NodeServices.layer)("bare destination resolution", (it) => {
  it.effect.each(["./ThreadLaunchService.ts", "../server.ts"])(
    "loads %s through native Node without relying on another entry point's import order",
    (entry) =>
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const node = yield* HostProcess.ExecutablePath;
        const code = yield* spawner.exitCode(
          ChildProcess.make(node, [
            "--input-type=module",
            "-e",
            "await import(process.argv[1])",
            new URL(entry, import.meta.url).href,
          ]),
        );
        expect(code).toBe(0);
      }),
  );

  it.effect(
    "delivers skill body only, in order, with exact hashes and no dependency discovery",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const dir = yield* fs.makeTempDirectoryScoped();
        const skill = `${dir}/SKILL.md`;
        const file = `${dir}/input.txt`;
        yield* fs.writeFileString(
          skill,
          "---\nname: testing\ndependencies: [secret]\n---\nRun focused tests.\n",
        );
        yield* fs.writeFileString(file, "literal input\n");
        const bare = yield* BareLaunch.BareLaunch;
        const resolved = yield* bare.resolve({
          request: {
            additions: [
              { type: "skillFile", path: skill },
              { type: "file", path: file },
            ],
          },
          instanceId,
          cwd: dir,
          readGrant: { type: "roots", roots: [dir] },
        });
        expect(resolved.additions).toHaveLength(2);
        expect(resolved.additions[0]).toMatchObject({ type: "skill", byteCount: 19 });
        expect(resolved.additions[0]).not.toHaveProperty("body");
        const text = yield* bare.deliveryText(resolved);
        expect(text).toContain("Run focused tests.\n");
        expect(text).not.toContain("dependencies");
        expect(text.indexOf("Run focused tests")).toBeLessThan(text.indexOf("literal input"));
        yield* fs.writeFileString(skill, "changed");
        expect((yield* bare.deliveryText(resolved).pipe(Effect.flip)).message).toContain(
          "changed since launch",
        );
      }).pipe(Effect.provide(fixtures())),
  );

  it.effect(
    "enforces read grants after realpath and refuses directories and oversized bodies",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const dir = yield* fs.makeTempDirectoryScoped();
        yield* fs.makeDirectory(`${dir}/allowed`);
        yield* fs.writeFileString(`${dir}/outside`, "private");
        yield* fs.symlink(`${dir}/outside`, `${dir}/allowed/escape`);
        const bare = yield* BareLaunch.BareLaunch;
        const resolve = (path: string, readGrant: BareReadGrant) =>
          bare.resolve({
            request: { additions: [{ type: "file", path }] },
            instanceId,
            cwd: `${dir}/allowed`,
            readGrant,
          });
        expect(
          (yield* resolve(`${dir}/allowed/escape`, {
            type: "roots",
            roots: [`${dir}/allowed`],
          }).pipe(Effect.flip)).message,
        ).toContain("outside");
        expect(
          (yield* resolve(`${dir}/outside`, { type: "none" }).pipe(Effect.flip)).message,
        ).toContain("filesystem:read");
        expect(
          (yield* resolve(`${dir}/allowed`, { type: "all" }).pipe(Effect.flip)).message,
        ).toContain("regular file");
        yield* fs.writeFileString(`${dir}/large`, "x".repeat(256 * 1024 + 1));
        expect(
          (yield* resolve(`${dir}/large`, { type: "all" }).pipe(Effect.flip)).message,
        ).toContain("256 KiB");
        expect((yield* resolve(`${dir}/outside`, { type: "all" })).additions).toHaveLength(1);
      }).pipe(Effect.provide(fixtures())),
  );

  it.effect(
    "resolves per-tool t3-code additions and rejects unknown tools before preparation",
    () =>
      Effect.gen(function* () {
        const bare = yield* BareLaunch.BareLaunch;
        const request = {
          additions: [
            { type: "mcp", server: "t3-code", tool: "preview_open" },
            { type: "mcp", server: "t3-code", tool: "mcp__t3-code__preview_snapshot" },
          ],
        } as const;
        yield* bare.validate({ request, instanceId, readGrant: { type: "none" } });
        expect(
          yield* bare.resolve({ request, instanceId, cwd: "/unused", readGrant: { type: "none" } }),
        ).toEqual({
          additions: [
            { type: "mcp", server: "t3-code", tool: "preview_open" },
            { type: "mcp", server: "t3-code", tool: "preview_snapshot" },
          ],
        });
        const invalid = {
          additions: [{ type: "mcp", server: "t3-code", tool: "missing_tool" }],
        } as const;
        for (const operation of [
          bare.validate({ request: invalid, instanceId }),
          bare.resolve({
            request: invalid,
            instanceId,
            cwd: "/unused",
            readGrant: { type: "none" },
          }),
        ])
          expect((yield* operation.pipe(Effect.flip)).message).toContain(
            "Unknown t3-code MCP tool: missing_tool",
          );
      }).pipe(Effect.provide(fixtures())),
  );

  it.effect("rejects unsupported integrations explicitly without reading their dependencies", () =>
    Effect.gen(function* () {
      const bare = yield* BareLaunch.BareLaunch;
      const requests: ReadonlyArray<BareLaunchRequest> = [
        { additions: [{ type: "plugin", name: "lint" }] },
        { additions: [{ type: "app", name: "docs" }] },
        { additions: [{ type: "mcp", server: "other" }] },
      ];
      for (const request of requests)
        expect(
          (yield* bare
            .resolve({ request, instanceId, cwd: "/unused", readGrant: { type: "none" } })
            .pipe(Effect.flip)).message,
        ).toContain("not supported");
      expect(
        yield* bare.resolve({
          request: { additions: [{ type: "mcp", server: "t3-code" }] },
          instanceId,
          cwd: "/unused",
          readGrant: { type: "none" },
        }),
      ).toEqual({ additions: [{ type: "mcp", server: "t3-code" }] });
    }).pipe(Effect.provide(fixtures())),
  );

  it.effect(
    "resolves a named skill from the destination cwd and rejects missing and ambiguous names",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const dir = yield* fs.makeTempDirectoryScoped();
        yield* fs.writeFileString(`${dir}/one.md`, "one");
        yield* fs.writeFileString(`${dir}/two.md`, "two");
        const providerLayer = fixtures(({ cwd }) =>
          Effect.succeed([
            {
              instanceId,
              workspaceSnapshots: [
                {
                  cwd,
                  skills: cwd.endsWith("missing")
                    ? []
                    : cwd.endsWith("ambiguous")
                      ? [
                          { name: "same", path: `${dir}/one.md` },
                          { name: "same", path: `${dir}/two.md` },
                        ]
                      : [
                          {
                            name: "same",
                            path: `${dir}/${cwd.endsWith("two") ? "two" : "one"}.md`,
                          },
                        ],
                },
              ],
            },
          ] as never),
        );
        yield* Effect.gen(function* () {
          const bare = yield* BareLaunch.BareLaunch;
          const resolve = (cwd: string) =>
            bare.resolve({
              request: { additions: [{ type: "skill", name: "same" }] },
              instanceId,
              cwd,
              readGrant: { type: "none" },
            });
          const one = yield* resolve(`${dir}/one`);
          const two = yield* resolve(`${dir}/two`);
          expect(one.additions[0]).not.toEqual(two.additions[0]);
          expect((yield* resolve(`${dir}/missing`).pipe(Effect.flip)).message).toContain("missing");
          expect((yield* resolve(`${dir}/ambiguous`).pipe(Effect.flip)).message).toContain(
            "ambiguous",
          );
        }).pipe(Effect.provide(providerLayer));
      }),
  );
});

describe("bare receipt", () => {
  it("labels planned removal separately from unobserved categories and bounds the existing notice", () => {
    const receipt = BareRuntime.bareReceipt({
      driver: ProviderDriverKind.make("grok"),
      bare: { additions: [] },
      mode: "approval-required",
      effective: "ask",
      controls: ["planned config"],
      retained: ["native rules".repeat(1000)],
      root: "/records",
      sessionId: "native",
    });
    expect(receipt.evidence.every((entry) => entry.status === "unobserved")).toBe(true);
    expect(BareRuntime.bareNoticeMessage(receipt).length).toBeLessThanOrEqual(2000);
    expect(BareRuntime.bareNoticeMessage(receipt)).toContain("Planned");
    expect(BareRuntime.bareNoticeMessage(receipt)).toContain("Unobserved:");
    expect(BareRuntime.bareNoticeMessage(receipt)).toContain("Native record: /records");
    expect(BareRuntime.bareNoticeMessage(receipt)).toContain("Native session: native");
  });
});
