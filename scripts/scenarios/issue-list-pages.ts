/// <reference lib="dom" />
// Scripted answers to the Issues view's paged GitHub reads; the DOM lib types the page code.
import type { Browser } from "@e2e-dev/web";

/**
 * What an Issue carries beyond its number. Type aliases here, not interfaces: only those pass as
 * the init script's JSON argument.
 */
type IssueFacts = {
  labels?: Array<string>;
  openBlockerCount?: number;
  /** The Issue it is a sub-Issue of, which the parent tree indents it under. */
  parent?: number;
  /** A pull request closing it; a review mark is the viewer's own, so it is trusted. */
  pullRequest?: {
    number: number;
    isDraft?: boolean;
    review?: "pending" | "success" | "failure";
    checks?: "pending" | "success" | "failure" | "none";
  };
  /** Ids of threads linked to it; with any, the helper answers the linked-thread reads itself. */
  threads?: Array<string>;
};

/** One answer to a page read: its Issue numbers and where the search carries on, or a failure. */
type IssuePageAnswer = {
  issues: Array<number>;
  next?: string;
  /** GitHub's match count for this read, when not every Issue of the plan: a narrower search. */
  matches?: number;
  /** How long the read takes to answer, for a list that is empty while it waits. */
  delayMs?: number;
  /** `rpc` fails the read itself; `github` answers it with GitHub's refusal inside the result. */
  failure?: "rpc" | "github";
};

/** One answer to a counts read: the totals of the listed repositories, or a failed read. */
type IssueCountsAnswer = { open: number; filed: number; closed: number } | "failure";

/**
 * Answers every `githubIssues.list` read in the page: `plan.first` for a listing and
 * `plan[cursor]` for a continuation. A read takes the next answer of its list; the last repeats.
 * The first `failedThreadReads` reads of the rows' linked threads fail; later ones reach the
 * server, unless `facts` links threads, which the helper then answers.
 */
export function answerIssueListPages(
  browser: Browser,
  plan: Record<string, Array<IssuePageAnswer>>,
  options: {
    failedThreadReads?: number;
    facts?: Record<number, IssueFacts>;
    /** One answer per `githubIssues.counts` read, the last repeating; absent, they reach the server. */
    counts?: Array<IssueCountsAnswer>;
  } = {},
) {
  return browser.addInitScript(
    ({ answersByPage, failedThreadReads, facts, counts }) => {
      const SEARCH = "github.com#0";
      const repository = {
        host: "github.com",
        repository: "example/demo",
        projectId: "demo",
        projectTitle: "demo",
      };
      // GitHub's match count for the search: every Issue the plan holds.
      const matches = new Set(
        Object.values(answersByPage).flatMap((answers) => answers.flatMap(({ issues }) => issues)),
      ).size;
      const taken = new Map<string, number>();
      let threadReads = 0;
      let countReads = 0;
      const send = WebSocket.prototype.send;
      WebSocket.prototype.send = function (data) {
        const request: {
          _tag: string;
          id: string;
          tag: string;
          payload: { cursors?: Record<string, string>; issues?: Array<{ number: number }> };
        } | null = typeof data === "string" ? JSON.parse(data) : null;
        const reply = (exit: unknown, delayMs = 0) => {
          const answer = () =>
            this.dispatchEvent(
              new MessageEvent("message", {
                data: JSON.stringify({ _tag: "Exit", requestId: request?.id, exit }),
              }),
            );
          // @effect-diagnostics-next-line globalTimers:off - This callback is injected into the browser.
          if (delayMs > 0) setTimeout(answer, delayMs);
          else queueMicrotask(answer);
        };
        const failure = (error: object) => ({ _tag: "Failure", cause: [{ _tag: "Fail", error }] });
        if (request?._tag === "Request" && request.tag === "issueLinks.threadsForIssues") {
          threadReads += 1;
          if (threadReads <= failedThreadReads) {
            return reply(
              failure({ _tag: "IssueLinkError", detail: "The links database is busy." }),
            );
          }
          if (!Object.values(facts).some(({ threads }) => threads !== undefined)) {
            return send.call(this, data);
          }
          return reply({
            _tag: "Success",
            value: {
              issues: (request.payload.issues ?? []).map(({ number }) => ({
                host: repository.host,
                repository: repository.repository,
                number,
                threads: (facts[number]?.threads ?? []).map((id) => ({
                  id,
                  projectId: repository.projectId,
                  title: `Thread on Issue ${number}`,
                  archivedAt: null,
                  sources: ["manual"],
                  pullRequests: [],
                })),
              })),
            },
          });
        }
        if (request?._tag === "Request" && request.tag === "githubIssues.counts" && counts) {
          const answer = counts[Math.min(countReads, counts.length - 1)];
          countReads += 1;
          return reply(
            answer === undefined || answer === "failure"
              ? failure({
                  _tag: "IssueOperationError",
                  operation: "countIssues",
                  detail: "GitHub did not answer the Issue counts in time.",
                })
              : { _tag: "Success", value: answer },
          );
        }
        if (request?._tag !== "Request" || request.tag !== "githubIssues.list") {
          return send.call(this, data);
        }
        const page = request.payload.cursors?.[SEARCH] ?? "first";
        const answers = answersByPage[page] ?? [];
        const read = taken.get(page) ?? 0;
        taken.set(page, read + 1);
        const answer = answers[Math.min(read, answers.length - 1)] ?? { issues: [] };
        reply(
          answer.failure === "rpc"
            ? failure({
                _tag: "IssueOperationError",
                operation: "listIssues",
                detail: "GitHub did not answer.",
              })
            : {
                _tag: "Success",
                value: {
                  repositories: [repository],
                  unsupported: [],
                  viewers: [{ host: "github.com", login: "octocat" }],
                  linkedPullRequests: [],
                  errors:
                    answer.failure === "github"
                      ? [{ host: "github.com", message: "GitHub API rate limit exceeded." }]
                      : [],
                  entries: answer.issues.map((number) => {
                    const {
                      labels = [],
                      openBlockerCount = 0,
                      parent,
                      pullRequest,
                    } = facts[number] ?? {};
                    return {
                      ...repository,
                      number,
                      title: `Open Issue ${number}`,
                      url: `https://github.com/example/demo/issues/${number}`,
                      state: "open",
                      author: null,
                      labels: labels.map((name) => ({ name, color: "ededed" })),
                      milestone: null,
                      commentCount: 0,
                      createdAt: "2026-10-01T00:00:00.000Z",
                      updatedAt: "2026-10-01T00:00:00.000Z",
                      parent:
                        parent === undefined
                          ? null
                          : {
                              host: repository.host,
                              repository: repository.repository,
                              number: parent,
                              title: `Open Issue ${parent}`,
                              url: `https://github.com/example/demo/issues/${parent}`,
                              state: "open",
                            },
                      subIssues: [],
                      subIssueCount: 0,
                      openBlockerCount,
                      closingPullRequests:
                        pullRequest === undefined
                          ? []
                          : [
                              {
                                host: repository.host,
                                repository: repository.repository,
                                number: pullRequest.number,
                                url: `https://github.com/example/demo/pull/${pullRequest.number}`,
                                state: "open",
                                isDraft: pullRequest.isDraft ?? false,
                                headRefName: `feat/${number}-work`,
                                headSha: "abc123",
                                review:
                                  pullRequest.review === undefined
                                    ? null
                                    : { state: pullRequest.review, creator: "octocat" },
                                ...(pullRequest.checks === undefined
                                  ? {}
                                  : { checks: pullRequest.checks }),
                              },
                            ],
                    };
                  }),
                  nextCursors: answer.next === undefined ? {} : { [SEARCH]: answer.next },
                  matchCounts:
                    answer.failure === "github" ? {} : { [SEARCH]: answer.matches ?? matches },
                },
              },
          answer.delayMs,
        );
      };
    },
    {
      answersByPage: plan,
      failedThreadReads: options.failedThreadReads ?? 0,
      facts: options.facts ?? {},
      counts: options.counts ?? null,
    },
  );
}
