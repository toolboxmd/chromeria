import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as DelegatedWorktreeBase from "./DelegatedWorktreeBase.ts";
import { git, layerRealGit, pushedCheckouts } from "./DelegatedWorktreeBase.testkit.ts";

const layer = Layer.mergeAll(layerRealGit, NodeServices.layer);
const has = (cwd: string, sha: string) => {
  try {
    git(cwd, "cat-file", "-e", `${sha}^{commit}`);
    return true;
  } catch {
    return false;
  }
};

it.live(
  "a pushed HEAD reaches another clone at that exact commit, not its branch's newer tip",
  () =>
    Effect.gen(function* () {
      const { parent, box, head } = yield* pushedCheckouts;
      expect(yield* DelegatedWorktreeBase.resolveBase(parent)).toBe(head);
      git(parent, "commit", "-q", "--allow-empty", "-m", "later");
      git(parent, "push", "-q");
      expect(has(box, head)).toBe(false);
      expect(yield* DelegatedWorktreeBase.fetchBase(box, head)).toBe(true);
      expect(has(box, head)).toBe(true);
      expect(has(box, git(parent, "rev-parse", "HEAD"))).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.live.each(["unpushed commit", "no upstream", "detached"] as const)(
  "refuses a parent whose HEAD is not on its upstream (%s)",
  (state) =>
    Effect.gen(function* () {
      const { parent } = yield* pushedCheckouts;
      if (state === "unpushed commit") git(parent, "commit", "-q", "--allow-empty", "-m", "local");
      if (state === "no upstream") git(parent, "checkout", "-q", "-b", "local");
      if (state === "detached") git(parent, "checkout", "-q", "--detach");
      const error = yield* Effect.flip(DelegatedWorktreeBase.resolveBase(parent));
      expect(error.code).toBe("invalid_request");
      expect(error.message).toContain("git push -u");
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.live("a base no remote has stays missing and is reported missing", () =>
  Effect.gen(function* () {
    const { parent, box } = yield* pushedCheckouts;
    git(parent, "commit", "-q", "--allow-empty", "-m", "local");
    const local = git(parent, "rev-parse", "HEAD");
    expect(yield* DelegatedWorktreeBase.fetchBase(box, local)).toBe(false);
    expect(has(box, local)).toBe(false);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);
