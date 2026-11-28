import { expect, it } from "@effect/vitest";
import { purposeAfterAgentLink } from "./ExecutionPurpose.ts";

const unscoped = { type: "unscoped", reason: "deliberate" } as const;
const issue = {
  type: "single",
  reference: { host: "github.com", repository: "example/repo", kind: "issue", number: 1 },
} as const;
const pr = { type: "single", reference: { ...issue.reference, kind: "pr" } } as const;
const issueUrl = "https://github.com/example/repo/issues/1";
const prUrl = "https://github.com/example/repo/pull/1";

it.each([
  [unscoped, "link", [issueUrl], issue],
  [unscoped, "link", [prUrl], pr],
  [unscoped, "link", [prUrl, prUrl + "2"], unscoped],
  [pr, "link", [issueUrl], issue],
  [issue, "link", [issueUrl + "2"], issue],
  [issue, "link", [prUrl], issue],
  [pr, "link", [prUrl + "2"], pr],
  [issue, "unlink", [issueUrl], unscoped],
  [pr, "unlink", [prUrl + "2", prUrl], unscoped],
  [issue, "unlink", [prUrl], issue],
  [unscoped, "link", ["https://gitlab.com/group/sub/project/-/merge_requests/42"], unscoped],
] as const)("resolves %j after %s %j", (scope, type, urls, expected) => {
  expect(purposeAfterAgentLink(scope, { type, urls })).toEqual(expected);
});
