import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";

import * as GitHubApi from "../sourceControl/GitHubApi.ts";
import * as ClosingReferences from "./closingReferences.ts";

const pullRequest = { host: "github.com", repository: "acme/web", number: 7 };

/** Each account sees a different Issue closed, so a reused entry shows up as the wrong one. */
const answerFor = (account: string) =>
  JSON.stringify({
    data: {
      repository: {
        pullRequest: {
          closingIssuesReferences: {
            nodes: [{ number: account === "a" ? 1 : 2, repository: { nameWithOwner: "acme/web" } }],
          },
        },
      },
    },
  });

const harness = Effect.gen(function* () {
  const account = yield* Ref.make("a");
  const reads = yield* Ref.make<ReadonlyArray<string | null>>([]);
  const api = Layer.mock(GitHubApi.GitHubApi)({
    credential: () =>
      Ref.get(account).pipe(
        Effect.map((name) => ({ token: Redacted.make(`token-${name}`), fingerprint: name })),
      ),
    graphql: () =>
      Effect.gen(function* () {
        const pinned = yield* GitHubApi.PinnedGitHubCredential;
        yield* Ref.update(reads, (all) => [...all, pinned?.credentialFingerprint ?? null]);
        return answerFor(pinned?.credentialFingerprint ?? "");
      }),
  });
  const closing = yield* Effect.provide(
    ClosingReferences.IssueClosingReferences,
    ClosingReferences.layer.pipe(Layer.provide(api)),
  );
  const closedBy = closing
    .issuesClosedBy({ pullRequest, version: "2026-09-01T00:00:00Z" })
    .pipe(Effect.map((keys) => keys.map((key) => key.number)));
  return { account, reads, closedBy };
});

describe("IssueClosingReferences", () => {
  it.effect("keeps each account's answer apart and reads under that account", () =>
    Effect.gen(function* () {
      const { account, reads, closedBy } = yield* harness;
      expect(yield* closedBy).toEqual([1]);
      expect(yield* closedBy).toEqual([1]);
      expect(yield* Ref.get(reads)).toEqual(["a"]);

      // `gh auth switch`: the other account's read is its own, never the cached one.
      yield* Ref.set(account, "b");
      expect(yield* closedBy).toEqual([2]);
      expect(yield* Ref.get(reads)).toEqual(["a", "b"]);

      yield* Ref.set(account, "a");
      expect(yield* closedBy).toEqual([1]);
      expect(yield* Ref.get(reads)).toEqual(["a", "b"]);
    }),
  );
});
