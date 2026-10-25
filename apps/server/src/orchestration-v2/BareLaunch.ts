import type { BareReadGrant } from "./bareReadGrant.ts";
import { BareLaunchError, unsupportedBare } from "@t3tools/provider-core/server/bareLaunch";
import { sha256 } from "@noble/hashes/sha2";
import { bytesToHex } from "@noble/hashes/utils";
import * as Crypto from "effect/Crypto";
import {
  type BareLaunchRequest,
  type ThreadBare,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import type { ThreadLaunchWorkspaceStrategy } from "./ThreadLaunchService.ts";
import * as Adapters from "./ProviderAdapterRegistry.ts";

/** Hash exactly what the harness receives, including a skill body's whitespace. */
function bareBody(text: string, skill: boolean): string {
  return skill
    ? text.replace(/^\uFEFF?---\r?\n[\s\S]*?\r?\n(?:---|\.\.\.)[ \t]*(?:\r?\n|$)/, "")
    : text;
}
const quoteAttribute = Schema.encodeSync(Schema.fromJsonString(Schema.String));
const digest = (text: string) => bytesToHex(sha256(new TextEncoder().encode(text)));
const MAX_BYTES = 256 * 1024;

export class BareLaunch extends Context.Service<
  BareLaunch,
  {
    readonly validate: (input: {
      readonly request: BareLaunchRequest;
      readonly instanceId: ProviderInstanceId;
      readonly readGrant?: BareReadGrant | undefined;
    }) => Effect.Effect<void, BareLaunchError>;
    readonly resolve: (input: {
      readonly request: BareLaunchRequest;
      readonly instanceId: ProviderInstanceId;
      readonly cwd: string;
      readonly readGrant: BareReadGrant;
    }) => Effect.Effect<ThreadBare, BareLaunchError>;
    readonly prepare: (input: {
      readonly request: BareLaunchRequest;
      readonly instanceId: ProviderInstanceId;
      readonly projectRoot: string;
      readonly workspaceStrategy: ThreadLaunchWorkspaceStrategy;
      readonly readGrant: BareReadGrant;
    }) => Effect.Effect<
      {
        readonly bare: ThreadBare;
        readonly workspaceStrategy: ThreadLaunchWorkspaceStrategy;
        readonly cwd: string;
      },
      BareLaunchError
    >;
    readonly deliveryText: (bare: ThreadBare) => Effect.Effect<string, BareLaunchError>;
  }
>()("t3/orchestration-v2/BareLaunch") {}

// Tool declarations depend on launch services; load their catalog only after
// module initialization, when a request actually names an individual tool.
const t3ToolCatalog = Effect.promise(() => import("../mcp/t3ToolNames.ts"));

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const git = yield* GitWorkflow.GitWorkflowService;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const providers = yield* ProviderRegistry.ProviderRegistry;
  const adapters = yield* Adapters.ProviderAdapterRegistryV2;
  const read = Effect.fn("BareLaunch.read")(function* (
    file: string,
    skill: boolean,
    grant: BareReadGrant,
  ) {
    if (grant.type === "none")
      return yield* new BareLaunchError({ reason: "Bare path additions require filesystem:read." });
    if (!path.isAbsolute(file))
      return yield* new BareLaunchError({
        reason: "Bare addition paths must be absolute on the destination environment.",
      });
    const real = yield* fs
      .realPath(file)
      .pipe(
        Effect.mapError(
          (cause) =>
            new BareLaunchError({ reason: `Bare addition ${file} is not readable.`, cause }),
        ),
      );
    if (grant.type === "roots") {
      const roots = yield* Effect.forEach(grant.roots, (root) =>
        fs
          .realPath(root)
          .pipe(
            Effect.mapError(
              (cause) => new BareLaunchError({ reason: "Bare read root is not readable.", cause }),
            ),
          ),
      );
      if (
        !roots.some((root) => {
          const relative = path.relative(root, real);
          return (
            relative === "" ||
            (!path.isAbsolute(relative) &&
              relative !== ".." &&
              !relative.startsWith(`..${path.sep}`))
          );
        })
      )
        return yield* new BareLaunchError({
          reason: `Bare addition ${file} is outside the caller's read roots.`,
        });
    }
    const info = yield* fs
      .stat(real)
      .pipe(
        Effect.mapError(
          (cause) =>
            new BareLaunchError({ reason: `Bare addition ${file} is not readable.`, cause }),
        ),
      );
    if (info.type !== "File")
      return yield* new BareLaunchError({
        reason: `Bare addition ${file} must be a regular file.`,
      });
    if (info.size > BigInt(MAX_BYTES))
      return yield* new BareLaunchError({ reason: "Bare additions exceed 256 KiB." });
    const raw = yield* fs
      .readFileString(real)
      .pipe(
        Effect.mapError(
          (cause) =>
            new BareLaunchError({ reason: `Bare addition ${file} is not readable.`, cause }),
        ),
      );
    const body = bareBody(raw, skill);
    return { path: real, body, sha256: digest(body), byteCount: Buffer.byteLength(body, "utf8") };
  });
  const validate: BareLaunch["Service"]["validate"] = Effect.fn("BareLaunch.validate")(
    function* (input) {
      const adapter = yield* adapters.get(input.instanceId).pipe(
        Effect.mapError(
          (cause) =>
            new BareLaunchError({
              reason: `Bare launch provider ${input.instanceId} is unavailable.`,
              cause,
            }),
        ),
      );
      const support =
        adapter.bareSupport === undefined
          ? unsupportedBare(adapter.driver)
          : yield* adapter.bareSupport;
      if (!support.supported)
        return yield* new BareLaunchError({
          reason: `Bare launch is not supported for ${adapter.driver}: ${support.reason}`,
        });
      for (const addition of input.request.additions ?? []) {
        const tool =
          addition.type === "mcp" && addition.tool !== undefined
            ? (yield* t3ToolCatalog).canonicalT3ToolName(addition.tool)
            : undefined;
        if (
          addition.type === "mcp" &&
          addition.server === "t3-code" &&
          addition.tool !== undefined &&
          tool === undefined
        )
          return yield* new BareLaunchError({
            reason: `Unknown t3-code MCP tool: ${addition.tool}.`,
          });
        if (
          input.readGrant?.type === "none" &&
          (addition.type === "file" || addition.type === "skillFile")
        )
          return yield* new BareLaunchError({
            reason: "Bare path additions require filesystem:read.",
          });
        if (
          addition.type === "plugin" ||
          addition.type === "app" ||
          (addition.type === "mcp" && (addition.server !== "t3-code" || adapter.driver === "grok"))
        )
          return yield* new BareLaunchError({
            reason: `Bare addition ${addition.type} is not supported on ${adapter.driver}: no proven exact attach path.`,
          });
      }
    },
  );
  const resolve: BareLaunch["Service"]["resolve"] = Effect.fn("BareLaunch.resolve")(
    function* (input) {
      const adapter = yield* adapters.get(input.instanceId).pipe(
        Effect.mapError(
          (cause) =>
            new BareLaunchError({
              reason: `Bare launch provider ${input.instanceId} is unavailable.`,
              cause,
            }),
        ),
      );
      const support =
        adapter.bareSupport === undefined
          ? unsupportedBare(adapter.driver)
          : yield* adapter.bareSupport;
      if (!support.supported)
        return yield* new BareLaunchError({
          reason: `Bare launch is not supported for ${adapter.driver}: ${support.reason}`,
        });
      const additions: Array<ThreadBare["additions"][number]> = [];
      let total = 0;
      for (const addition of input.request.additions ?? []) {
        const tool =
          addition.type === "mcp" && addition.tool !== undefined
            ? (yield* t3ToolCatalog).canonicalT3ToolName(addition.tool)
            : undefined;
        if (
          addition.type === "mcp" &&
          addition.server === "t3-code" &&
          addition.tool !== undefined &&
          tool === undefined
        )
          return yield* new BareLaunchError({
            reason: `Unknown t3-code MCP tool: ${addition.tool}.`,
          });
        if (addition.type === "mcp") {
          if (addition.server !== "t3-code" || adapter.driver === "grok")
            return yield* new BareLaunchError({
              reason: `Bare addition MCP ${addition.server}${addition.tool ? `/${addition.tool}` : ""} is not supported on ${adapter.driver}: no proven exact attach path.`,
            });
          additions.push({
            type: "mcp",
            server: "t3-code",
            ...(addition.tool === undefined ? {} : { tool }),
          });
          continue;
        }
        if (addition.type === "app" || addition.type === "plugin")
          return yield* new BareLaunchError({
            reason: `Bare addition ${addition.type} ${addition.name} is not supported on ${adapter.driver}: no single-addition attach path without user settings.`,
          });
        let file: string;
        if (addition.type === "skill") {
          const snapshots = yield* providers.refreshWorkspaceSnapshot({
            instanceId: input.instanceId,
            cwd: input.cwd,
            fresh: true,
          });
          const inventory =
            snapshots
              .find((provider) => provider.instanceId === input.instanceId)
              ?.workspaceSnapshots?.find((snapshot) => snapshot.cwd === input.cwd)?.skills ?? [];
          const matches = inventory.filter((skill) => skill.name === addition.name);
          if (matches.length !== 1)
            return yield* new BareLaunchError({
              reason: `Bare skill ${addition.name} is ${matches.length === 0 ? "missing" : "ambiguous"} in ${input.cwd}.`,
            });
          file = matches[0]!.path;
        } else file = addition.path;
        const loaded = yield* read(
          file,
          addition.type !== "file",
          addition.type === "skill" ? { type: "all" } : input.readGrant,
        );
        total += loaded.byteCount;
        if (total > MAX_BYTES)
          return yield* new BareLaunchError({ reason: "Bare additions exceed 256 KiB." });
        additions.push({
          type: addition.type === "file" ? "file" : "skill",
          ...(addition.type === "skill" ? { name: addition.name } : {}),
          path: loaded.path,
          sha256: loaded.sha256,
          byteCount: loaded.byteCount,
        });
      }
      return { additions };
    },
  );
  const deliveryText: BareLaunch["Service"]["deliveryText"] = Effect.fn("BareLaunch.deliveryText")(
    function* (bare) {
      const blocks: Array<string> = [];
      for (const addition of bare.additions) {
        if (addition.type === "mcp") continue;
        const loaded = yield* read(addition.path, addition.type === "skill", { type: "all" });
        if (
          loaded.sha256 !== addition.sha256 ||
          loaded.byteCount !== addition.byteCount ||
          loaded.path !== addition.path
        )
          return yield* new BareLaunchError({
            reason: `Bare addition ${addition.path} changed since launch.`,
          });
        // JSON attribute values keep paths from manufacturing another envelope.
        blocks.push(
          `<${addition.type} path=${quoteAttribute(addition.path)} sha256=${quoteAttribute(addition.sha256)}${addition.type === "skill" && addition.name ? ` name=${quoteAttribute(addition.name)}` : ""}>\n${loaded.body}\n</${addition.type}>`,
        );
      }
      return blocks.join("\n\n");
    },
  );
  const prepare: BareLaunch["Service"]["prepare"] = Effect.fn("BareLaunch.prepare")(
    function* (input) {
      yield* validate(input);
      let createdPath: string | undefined;
      let workspaceStrategy = input.workspaceStrategy;
      if (workspaceStrategy.type === "worktree") {
        let startRef = workspaceStrategy.baseRef;
        const provisionError = (cause: unknown) =>
          new BareLaunchError({ reason: "Bare workspace preparation failed.", cause });
        if (
          workspaceStrategy.startFromOrigin &&
          (yield* git
            .remoteExists({ cwd: input.projectRoot, remoteName: "origin" })
            .pipe(Effect.mapError(provisionError)))
        ) {
          yield* git
            .fetchRemote({ cwd: input.projectRoot, remoteName: "origin", refName: startRef })
            .pipe(Effect.mapError(provisionError));
          if (
            yield* git
              .remoteBranchExists({
                cwd: input.projectRoot,
                remoteName: "origin",
                refName: startRef,
              })
              .pipe(Effect.mapError(provisionError))
          )
            startRef = (yield* git
              .resolveRemoteTrackingCommit({
                cwd: input.projectRoot,
                refName: startRef,
                fallbackRemoteName: "origin",
              })
              .pipe(Effect.mapError(provisionError))).commitSha;
        }
        const worktree = yield* git
          .createWorktree({
            cwd: input.projectRoot,
            refName: startRef,
            newRefName:
              workspaceStrategy.branch ??
              `t3/${(yield* crypto.randomUUIDv4().pipe(Effect.orDie)).replaceAll("-", "")}`,
            baseRefName: workspaceStrategy.baseRef,
            path: null,
          })
          .pipe(Effect.mapError(provisionError));
        createdPath = worktree.worktree.path;
        workspaceStrategy = {
          type: "existing_worktree",
          worktreePath: worktree.worktree.path,
          branch: worktree.worktree.refName,
        };
      }
      const cwd =
        workspaceStrategy.type === "existing_worktree"
          ? workspaceStrategy.worktreePath
          : input.projectRoot;
      const grant =
        input.readGrant.type === "roots"
          ? { type: "roots" as const, roots: [...input.readGrant.roots, cwd] }
          : input.readGrant;
      const bare = yield* resolve({
        request: input.request,
        instanceId: input.instanceId,
        cwd,
        readGrant: grant,
      }).pipe(
        Effect.onError(() =>
          createdPath === undefined
            ? Effect.void
            : git
                .removeWorktree({ cwd: input.projectRoot, path: createdPath, force: false })
                .pipe(Effect.ignore),
        ),
      );
      return { bare, workspaceStrategy, cwd };
    },
  );
  return BareLaunch.of({ validate, resolve, prepare, deliveryText });
});
export const layer = Layer.effect(BareLaunch, make);
