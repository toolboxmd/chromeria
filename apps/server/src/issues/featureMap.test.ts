import { describe, expect, it } from "@effect/vitest";

import {
  featureMapFactsDocument,
  featureMapFactsOf,
  FEATURE_MAP_OPEN_ISSUE_PAGE,
  parseFeatureMapFile,
} from "./featureMap.ts";

const at = { host: "github.com", repository: "toolboxmd/contextmd" };

const file = (body: string) => `# Map\n\nIntro.\n\n\`\`\`mermaid\n${body}\n\`\`\`\n`;

describe("parseFeatureMapFile", () => {
  it("reads parts, their refs in order, and context parts", () => {
    const parsed = parseFeatureMapFile(
      file(`%%{init: {"theme": "dark"}}%%
flowchart TD
    S["Session in Chromeria"]
    %% S: context
    C["2 Capture: extract and judge"]
    %% C: #7 PR #22
    MK["Must-know rule before its action"]
    %% MK: toolboxmd/chromeria PR #233 toolboxmd/chromeria#231
    LJ["6 Labeler judge runs"]
    S --> C --> MK
    C -->|"next"| LJ`),
      at,
    );
    if (parsed === null) throw new Error("expected a diagram");
    expect(parsed.source.startsWith("%%{init")).toBe(true);
    expect(parsed.source.endsWith('C -->|"next"| LJ')).toBe(true);
    expect(parsed.parts).toEqual([
      { id: "S", name: "Session in Chromeria", context: true, primaryUnreadable: false, refs: [] },
      {
        id: "C",
        name: "2 Capture: extract and judge",
        context: false,
        primaryUnreadable: false,
        refs: [
          {
            kind: "issue",
            repository: "toolboxmd/contextmd",
            number: 7,
            url: "https://github.com/toolboxmd/contextmd/issues/7",
          },
          {
            kind: "pull",
            repository: "toolboxmd/contextmd",
            number: 22,
            url: "https://github.com/toolboxmd/contextmd/pull/22",
          },
        ],
      },
      {
        id: "MK",
        name: "Must-know rule before its action",
        context: false,
        primaryUnreadable: false,
        refs: [
          {
            kind: "pull",
            repository: "toolboxmd/chromeria",
            number: 233,
            url: "https://github.com/toolboxmd/chromeria/pull/233",
          },
          {
            kind: "issue",
            repository: "toolboxmd/chromeria",
            number: 231,
            url: "https://github.com/toolboxmd/chromeria/issues/231",
          },
        ],
      },
      {
        id: "LJ",
        name: "6 Labeler judge runs",
        context: false,
        primaryUnreadable: false,
        refs: [],
      },
    ]);
    expect(parsed.undeclaredRefLines).toEqual([]);
    expect(parsed.unreadableRefs).toEqual([]);
  });

  it("binds a ref line written before its part's declaration", () => {
    const parsed = parseFeatureMapFile(file(`flowchart TD\n    %% A: #1\n    A["Alpha"]`), at);
    expect(parsed?.parts.map((part) => [part.id, part.refs.map((ref) => ref.number)])).toEqual([
      ["A", [1]],
    ]);
  });

  it("reports ref lines for undeclared IDs and tokens it cannot read, keeping the rest", () => {
    const parsed = parseFeatureMapFile(
      file(`flowchart TD
    A["Alpha"]
    %% A: #3 issue-9 PR #4 PR
    %% GHOST: #5`),
      at,
    );
    expect(parsed?.parts[0]?.refs.map((ref) => `${ref.kind} ${ref.number}`)).toEqual([
      "issue 3",
      "pull 4",
    ]);
    expect(parsed?.undeclaredRefLines).toEqual([{ id: "GHOST", text: "#5" }]);
    expect(parsed?.unreadableRefs).toEqual([
      { id: "A", text: "issue-9" },
      { id: "A", text: "PR" },
    ]);
  });

  it("marks a part whose primary ref cannot be read, instead of promoting the next ref", () => {
    const parsed = parseFeatureMapFile(
      file(`flowchart TD\n    A["Alpha"]\n    %% A: #0 PR #38\n    B["Beta"]\n    %% B: #1 bogus`),
      at,
    );
    expect(parsed?.parts.map((part) => [part.id, part.primaryUnreadable])).toEqual([
      ["A", true],
      ["B", false],
    ]);
  });

  it("finds no diagram in a file without a mermaid fence", () => {
    expect(parseFeatureMapFile("# Map\n\n```text\nA --> B\n```\n", at)).toBeNull();
  });
});

describe("featureMapFactsDocument", () => {
  it("reads every ref as either type and the repository's open Issues, all as variables", () => {
    const document = featureMapFactsDocument({
      refs: [
        { kind: "issue", repository: "toolboxmd/contextmd", number: 7, url: "u7" },
        { kind: "pull", repository: "toolboxmd/contextmd", number: 22, url: "u22" },
      ],
      repository: "toolboxmd/contextmd",
      after: null,
    });
    expect(document.query).toContain("ref0: resource(url: $ref0_url)");
    expect(document.query).toContain("ref1: resource(url: $ref1_url)");
    expect(document.query).toContain("... on Issue");
    expect(document.query).toContain("... on PullRequest");
    expect(document.query).toContain(`issues(states: OPEN, first: ${FEATURE_MAP_OPEN_ISSUE_PAGE}`);
    expect(document.variables).toMatchObject({
      ref0_url: "u7",
      ref1_url: "u22",
      owner: "toolboxmd",
      name: "contextmd",
      after: null,
    });
  });

  it("reads only the open Issues' next page when there are no refs left", () => {
    const document = featureMapFactsDocument({
      refs: [],
      repository: "toolboxmd/contextmd",
      after: "cursor",
    });
    expect(document.query).not.toContain("resource(");
    expect(document.variables).toMatchObject({ after: "cursor" });
  });
});

describe("featureMapFactsOf", () => {
  const refs = [
    { kind: "issue", repository: "toolboxmd/contextmd", number: 7, url: "u7" },
    { kind: "pull", repository: "toolboxmd/contextmd", number: 38, url: "u38" },
    { kind: "pull", repository: "toolboxmd/contextmd", number: 40, url: "u40" },
    { kind: "issue", repository: "lukemaj/private", number: 2, url: "u2" },
    { kind: "issue", repository: "toolboxmd/contextmd", number: 44, url: "u44" },
  ] as const;

  it("folds each answer into a fact, and anything GitHub did not answer into null", () => {
    const facts = featureMapFactsOf(refs, {
      ref0: {
        __typename: "Issue",
        url: "https://github.com/toolboxmd/contextmd/issues/7",
        state: "OPEN",
        stateReason: null,
        labels: { pageInfo: { hasNextPage: false }, nodes: [{ name: "bug" }] },
        closedByPullRequestsReferences: {
          pageInfo: { hasNextPage: false },
          nodes: [
            {
              number: 22,
              url: "https://github.com/toolboxmd/contextmd/pull/22",
              state: "OPEN",
              isDraft: true,
              repository: { nameWithOwner: "toolboxmd/contextmd" },
              commits: { nodes: [{ commit: { statusCheckRollup: { state: "FAILURE" } } }] },
            },
            {
              number: 21,
              url: "https://github.com/toolboxmd/contextmd/pull/21",
              state: "CLOSED",
              isDraft: false,
              repository: { nameWithOwner: "toolboxmd/contextmd" },
              commits: { nodes: [] },
            },
            null,
          ],
        },
      },
      ref1: {
        __typename: "PullRequest",
        url: "https://github.com/toolboxmd/contextmd/pull/38",
        state: "MERGED",
        isDraft: false,
        baseRefName: "main",
        baseRepository: { defaultBranchRef: { name: "main" } },
        commits: { nodes: [{ commit: { statusCheckRollup: { state: "SUCCESS" } } }] },
      },
      ref2: {
        __typename: "PullRequest",
        url: "https://github.com/toolboxmd/contextmd/pull/40",
        state: "MERGED",
        isDraft: false,
        baseRefName: "feat/stack-base",
        baseRepository: { defaultBranchRef: { name: "main" } },
        commits: { nodes: [{ commit: { statusCheckRollup: null } }] },
      },
      ref3: null,
      // A resource of neither type answers an empty object.
      ref4: {},
    });
    expect(facts).toEqual([
      {
        repository: "toolboxmd/contextmd",
        number: 7,
        fact: {
          kind: "issue",
          url: "https://github.com/toolboxmd/contextmd/issues/7",
          state: "open",
          labels: ["bug"],
          complete: true,
          openClosingPullRequests: [
            {
              repository: "toolboxmd/contextmd",
              number: 22,
              url: "https://github.com/toolboxmd/contextmd/pull/22",
              isDraft: true,
              checks: "failing",
            },
          ],
        },
      },
      {
        repository: "toolboxmd/contextmd",
        number: 38,
        fact: {
          kind: "pull",
          url: "https://github.com/toolboxmd/contextmd/pull/38",
          state: "merged",
          isDraft: false,
          mergedIntoDefault: true,
          checks: "passing",
        },
      },
      {
        repository: "toolboxmd/contextmd",
        number: 40,
        fact: {
          kind: "pull",
          url: "https://github.com/toolboxmd/contextmd/pull/40",
          state: "merged",
          isDraft: false,
          mergedIntoDefault: false,
          checks: null,
        },
      },
      { repository: "lukemaj/private", number: 2, fact: null },
      { repository: "toolboxmd/contextmd", number: 44, fact: null },
    ]);
  });

  it("marks an Issue incomplete when GitHub holds more labels or closing PRs than it returned", () => {
    const answer = (labelsMore: boolean, closingMore: boolean) => ({
      ref0: {
        __typename: "Issue",
        url: "u",
        state: "OPEN",
        stateReason: null,
        labels: { pageInfo: { hasNextPage: labelsMore }, nodes: [{ name: "area" }] },
        closedByPullRequestsReferences: { pageInfo: { hasNextPage: closingMore }, nodes: [] },
      },
    });
    const completeOf = (labelsMore: boolean, closingMore: boolean) => {
      const fact = featureMapFactsOf([refs[0]], answer(labelsMore, closingMore))[0]?.fact;
      return fact?.kind === "issue" ? fact.complete : null;
    };
    expect(completeOf(false, false)).toBe(true);
    expect(completeOf(true, false)).toBe(false);
    expect(completeOf(false, true)).toBe(false);
  });

  it("reads a closed-not-planned Issue as not planned", () => {
    const [entry] = featureMapFactsOf([refs[0]], {
      ref0: {
        __typename: "Issue",
        url: "u",
        state: "CLOSED",
        stateReason: "NOT_PLANNED",
        labels: { pageInfo: { hasNextPage: false }, nodes: [] },
        closedByPullRequestsReferences: { pageInfo: { hasNextPage: false }, nodes: [] },
      },
    });
    expect(entry?.fact).toMatchObject({ kind: "issue", state: "not-planned" });
  });
});
