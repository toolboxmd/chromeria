// @effect-diagnostics nodeBuiltinImport:off -- Builds Git fixtures with plain synchronous git calls.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";

import * as ServerConfig from "../config.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";

export const git = (cwd: string, ...args: ReadonlyArray<string>) =>
  NodeChildProcess.execFileSync("git", args, {
    cwd,
    encoding: "utf8",
    stdio: "pipe",
    env: {
      ...process.env,
      GIT_CONFIG_GLOBAL: "/dev/null",
      GIT_CONFIG_NOSYSTEM: "1",
      GIT_AUTHOR_NAME: "Test",
      GIT_AUTHOR_EMAIL: "test@example.com",
      GIT_COMMITTER_NAME: "Test",
      GIT_COMMITTER_EMAIL: "test@example.com",
    },
  }).trim();

export const layerRealGit = GitVcsDriver.layer.pipe(
  Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "delegated-worktree-base-" })),
  Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);

/**
 * `parent` on branch `feature`, its HEAD pushed to the bare `origin`, and `box`, a clone of
 * `origin` made before that HEAD was pushed.
 */
export const pushedCheckouts = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const dir = yield* fileSystem.realPath(
    yield* fileSystem.makeTempDirectoryScoped({ prefix: "delegated-worktree-base-" }),
  );
  const origin = `${dir}/origin.git`;
  const parent = `${dir}/parent`;
  const box = `${dir}/box`;
  git(dir, "init", "-q", "--bare", "-b", "main", origin);
  git(dir, "clone", "-q", origin, parent);
  git(parent, "checkout", "-q", "-b", "feature");
  git(parent, "commit", "-q", "--allow-empty", "-m", "first");
  git(parent, "push", "-q", "-u", "origin", "feature");
  git(dir, "clone", "-q", "-b", "feature", origin, box);
  git(parent, "commit", "-q", "--allow-empty", "-m", "pushed");
  git(parent, "push", "-q");
  return { parent, box, head: git(parent, "rev-parse", "HEAD") };
});
