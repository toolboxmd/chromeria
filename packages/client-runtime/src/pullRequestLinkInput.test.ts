import { EnvironmentId, ThreadId, type ThreadPullRequestLink } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";

import { sourceControlClients } from "./sourceControlClients.ts";

import {
  createThreadPullRequestWatchAction,
  parsePullRequestReference,
  resolveLinkPullRequestInput,
  threadPullRequestWatchInput,
} from "./pullRequestLinkInput.ts";

const changeRequestWebUrl = (kind: string, host: string, repository: string, number: number) =>
  sourceControlClients.get(kind).changeRequestUrl({ host, repository, number });

const project = {
  host: "github.com",
  repository: "acme/web",
  webUrl: (number: number) => changeRequestWebUrl("github", "github.com", "acme/web", number),
};

describe("resolveLinkPullRequestInput", () => {
  it.each([
    ["https://bitbucket.org/acme/web/pull-requests/42", "bitbucket.org"],
    ["https://github.acme.test/acme/web/pull/42", "github.acme.test"],
    ["https://git.acme.test/acme/web/-/merge_requests/42", "git.acme.test"],
  ])("links supported host URL %s without a thread project", (url, host) => {
    expect(
      resolveLinkPullRequestInput({
        reference: ` ${url} `,
        project: null,
        hasProject: (candidate) => candidate.host === host,
      }),
    ).toEqual({ link: { host, repository: "acme/web", number: 42, url } });
  });

  it("validates the full Azure repository when resolving a browser URL", () => {
    const hasProject = (reference: { host: string; repository: string }) =>
      reference.host === "dev.azure.com" && reference.repository === "org-a/project/_git/web";
    expect(
      resolveLinkPullRequestInput({
        reference: "https://dev.azure.com/org-a/project/_git/web/pullrequest/42",
        project: null,
        hasProject,
      }),
    ).toMatchObject({ link: { repository: "org-a/project/_git/web", number: 42 } });
    expect(
      resolveLinkPullRequestInput({
        reference: "https://dev.azure.com/org-b/project/_git/web/pullrequest/42",
        project: null,
        hasProject,
      }),
    ).toMatchObject({ error: expect.stringContaining("org-b/project/_git/web") });
  });

  it("resolves bare Azure numbers into canonical browser URLs", () => {
    expect(
      resolveLinkPullRequestInput({
        reference: "#42",
        project: {
          host: "ssh.dev.azure.com",
          repository: "v3/org/project/web",
          webUrl: (number) =>
            changeRequestWebUrl("azure-devops", "ssh.dev.azure.com", "v3/org/project/web", number),
        },
        hasProject: () => true,
      }),
    ).toMatchObject({
      link: {
        host: "dev.azure.com",
        repository: "org/project/_git/web",
        number: 42,
        url: "https://dev.azure.com/org/project/_git/web/pullrequest/42",
      },
    });
  });

  it("returns null for input that is not a reference", () => {
    expect(
      resolveLinkPullRequestInput({ reference: "hello", project, hasProject: () => true }),
    ).toBeNull();
  });

  it("resolves a bare number against the thread's own repository", () => {
    expect(
      resolveLinkPullRequestInput({ reference: "#42", project, hasProject: () => true }),
    ).toEqual({
      link: {
        host: "github.com",
        repository: "acme/web",
        number: 42,
        url: "https://github.com/acme/web/pull/42",
      },
    });
  });

  it("links a URL from another repository on a host with a project", () => {
    expect(
      resolveLinkPullRequestInput({
        reference: "https://github.com/acme/api/pull/7",
        project,
        hasProject: (reference) => reference.host === "github.com",
      }),
    ).toEqual({
      link: {
        host: "github.com",
        repository: "acme/api",
        number: 7,
        url: "https://github.com/acme/api/pull/7",
      },
    });
  });

  it("refuses a URL on a host nothing is checked out from", () => {
    const result = resolveLinkPullRequestInput({
      reference: "https://gitlab.com/acme/api/-/merge_requests/7",
      project,
      hasProject: () => false,
    });
    expect(result).toMatchObject({ error: expect.stringContaining("gitlab.com") });
  });

  it("asks for a URL when a bare number has no project to resolve against", () => {
    expect(
      resolveLinkPullRequestInput({ reference: "12", project: null, hasProject: () => true }),
    ).toMatchObject({ error: expect.stringContaining("full URL") });
  });

  it("accepts a checkout command as a reference", () => {
    expect(
      resolveLinkPullRequestInput({
        reference: "gh pr checkout https://github.com/acme/web/pull/3",
        project,
        hasProject: () => true,
      }),
    ).toMatchObject({ link: { number: 3, repository: "acme/web" } });
  });
});

describe("changeRequestWebUrl", () => {
  it("knows the four hosts and nothing else", () => {
    expect(changeRequestWebUrl("gitlab", "gitlab.com", "g/sub/repo", 5)).toBe(
      "https://gitlab.com/g/sub/repo/-/merge_requests/5",
    );
    expect(changeRequestWebUrl("unknown", "x", "a/b", 1)).toBeNull();
  });
});

describe("parsePullRequestReference", () => {
  it("accepts GitHub pull request URLs", () => {
    expect(parsePullRequestReference("https://github.com/pingdotgg/t3code/pull/42")).toBe(
      "https://github.com/pingdotgg/t3code/pull/42",
    );
  });

  it("accepts Azure DevOps pull request URLs", () => {
    expect(
      parsePullRequestReference("https://dev.azure.com/acme/project/_git/t3code/pullrequest/42"),
    ).toBe("https://dev.azure.com/acme/project/_git/t3code/pullrequest/42");
  });

  it("accepts GitLab merge request URLs", () => {
    expect(parsePullRequestReference("https://gitlab.com/group/project/-/merge_requests/42")).toBe(
      "https://gitlab.com/group/project/-/merge_requests/42",
    );
  });

  it("accepts legacy Azure DevOps pull request URLs", () => {
    expect(
      parsePullRequestReference("https://acme.visualstudio.com/project/_git/t3code/pullrequest/42"),
    ).toBe("https://acme.visualstudio.com/project/_git/t3code/pullrequest/42");
  });

  it("accepts raw numbers", () => {
    expect(parsePullRequestReference("42")).toBe("42");
  });

  it("accepts #number references", () => {
    expect(parsePullRequestReference("#42")).toBe("42");
  });

  it("accepts gh pr checkout commands with raw numbers", () => {
    expect(parsePullRequestReference("gh pr checkout 42")).toBe("42");
  });

  it("accepts gh pr checkout commands with #number references", () => {
    expect(parsePullRequestReference("gh pr checkout #42")).toBe("42");
  });

  it("accepts gh pr checkout commands with GitHub pull request URLs", () => {
    expect(
      parsePullRequestReference("gh pr checkout https://github.com/pingdotgg/t3code/pull/42"),
    ).toBe("https://github.com/pingdotgg/t3code/pull/42");
  });

  it("accepts glab mr checkout commands with raw numbers", () => {
    expect(parsePullRequestReference("glab mr checkout 42")).toBe("42");
  });

  it("accepts az repos pr checkout commands with raw numbers", () => {
    expect(parsePullRequestReference("az repos pr checkout --id 42")).toBe("42");
  });

  it("accepts az repos pr checkout commands with equals-style ids", () => {
    expect(parsePullRequestReference("az repos pr checkout --id=42")).toBe("42");
  });

  it("accepts az repos pr checkout commands with extra flags", () => {
    expect(parsePullRequestReference("az repos pr checkout --id 42 --remote-name origin")).toBe(
      "42",
    );
  });

  it("rejects non-pull-request input", () => {
    expect(parsePullRequestReference("feature/my-branch")).toBeNull();
  });
});

describe("threadPullRequestWatchInput", () => {
  const threadId = ThreadId.make("parent");
  const reference: ThreadPullRequestLink = {
    host: "github.com",
    repository: "acme/web",
    number: 42,
    url: "https://github.com/acme/web/pull/42",
    source: "agent",
    linkedAt: "2026-10-18T20:00:00.000Z",
    snapshot: null,
    stack: null,
  };
  const watchInput = {
    threadId,
    host: "github.com",
    repository: "acme/web",
    number: 42,
    watching: true,
  };

  it("sends parent link intent for a descendant-only PR", () => {
    expect(
      threadPullRequestWatchInput({ threadId, reference, ownLinks: [], watching: true }),
    ).toEqual({ ...watchInput, link: { url: reference.url, source: "manual" } });
  });

  it("preserves a direct link without relinking it", () => {
    expect(
      threadPullRequestWatchInput({ threadId, reference, ownLinks: [reference], watching: true }),
    ).toEqual(watchInput);
  });

  it("recognizes direct links with normalized host and repository names", () => {
    expect(
      threadPullRequestWatchInput({
        threadId,
        reference,
        ownLinks: [{ ...reference, host: "GitHub.com", repository: "Acme/Web" }],
        watching: true,
      }),
    ).toEqual(watchInput);
  });

  it.each([{ host: "git.example.com" }, { repository: "other/web" }, { number: 43 }])(
    "does not mistake another PR for a direct link: %j",
    (other) => {
      expect(
        threadPullRequestWatchInput({
          threadId,
          reference,
          ownLinks: [{ ...reference, ...other }],
          watching: true,
        }),
      ).toEqual({ ...watchInput, link: { url: reference.url, source: "manual" } });
    },
  );

  it.each([[[]], [[reference]]])("never creates a link when stopping a watch", (ownLinks) => {
    expect(threadPullRequestWatchInput({ threadId, reference, ownLinks, watching: false })).toEqual(
      { ...watchInput, watching: false },
    );
  });
});

describe("createThreadPullRequestWatchAction", () => {
  const threadRef = {
    environmentId: EnvironmentId.make("parent-environment"),
    threadId: ThreadId.make("parent"),
  };
  const descendant = {
    id: ThreadId.make("child"),
    pullRequests: [
      {
        host: "github.com",
        repository: "acme/web",
        number: 42,
        url: "https://github.com/acme/web/pull/42",
        source: "agent",
        linkedAt: "2026-10-18T20:00:00.000Z",
        snapshot: null,
        stack: null,
      } satisfies ThreadPullRequestLink,
    ],
  };
  const reference = descendant.pullRequests[0]!;

  it("sends the parent's Watch command for a descendant-only row", async () => {
    const result = { _tag: "Success" };
    const watch = vi.fn(() => Promise.resolve(result));
    const onSetWatching = createThreadPullRequestWatchAction({ threadRef, ownLinks: [], watch });
    await expect(onSetWatching(reference, true)).resolves.toBe(result);
    expect(watch).toHaveBeenCalledOnce();
    expect(watch).toHaveBeenCalledWith({
      environmentId: "parent-environment",
      input: {
        threadId: "parent",
        host: "github.com",
        repository: "acme/web",
        number: 42,
        watching: true,
        link: { url: "https://github.com/acme/web/pull/42", source: "manual" },
      },
    });
  });

  it.each([
    { watching: true, ownLinks: [reference] },
    { watching: false, ownLinks: [] },
  ])(
    "sends no link intent for a direct PR or Stop watching: %j",
    async ({ watching, ownLinks }) => {
      const watch = vi.fn(() => Promise.resolve());
      const onSetWatching = createThreadPullRequestWatchAction({ threadRef, ownLinks, watch });
      await onSetWatching(reference, watching);
      expect(watch).toHaveBeenCalledOnce();
      expect(watch).toHaveBeenCalledWith({
        environmentId: "parent-environment",
        input: {
          threadId: "parent",
          host: "github.com",
          repository: "acme/web",
          number: 42,
          watching,
        },
      });
    },
  );
});
