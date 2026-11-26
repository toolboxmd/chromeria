/// <reference lib="dom" />
// Scripted answers to the Issues view's paged GitHub reads; the DOM lib types the page code.
import type { Browser } from "@e2e-dev/web";

/**
 * One answer to a page read: its Issue numbers and where the search carries on, or a failure.
 * A type alias, not an interface: only that passes as the init script's JSON argument.
 */
type IssuePageAnswer = {
  issues: Array<number>;
  next?: string;
  /** `rpc` fails the read itself; `github` answers it with GitHub's refusal inside the result. */
  failure?: "rpc" | "github";
};

/**
 * Answers every `githubIssues.list` read in the page: `plan.first` for a listing and
 * `plan[cursor]` for a continuation. A read takes the next answer of its list; the last repeats.
 * The first `failedThreadReads` reads of the rows' linked threads fail; later ones reach the server.
 */
export function answerIssueListPages(
  browser: Browser,
  plan: Record<string, Array<IssuePageAnswer>>,
  failedThreadReads = 0,
) {
  return browser.addInitScript(
    ({ answersByPage, failedThreadReads }) => {
      const SEARCH = "github.com#0";
      const repository = {
        host: "github.com",
        repository: "example/demo",
        projectId: "demo",
        projectTitle: "demo",
      };
      const taken = new Map<string, number>();
      let threadReads = 0;
      const send = WebSocket.prototype.send;
      WebSocket.prototype.send = function (data) {
        const request: {
          _tag: string;
          id: string;
          tag: string;
          payload: { cursors?: Record<string, string> };
        } | null = typeof data === "string" ? JSON.parse(data) : null;
        const reply = (exit: unknown) =>
          queueMicrotask(() =>
            this.dispatchEvent(
              new MessageEvent("message", {
                data: JSON.stringify({ _tag: "Exit", requestId: request?.id, exit }),
              }),
            ),
          );
        const failure = (error: object) => ({ _tag: "Failure", cause: [{ _tag: "Fail", error }] });
        if (request?._tag === "Request" && request.tag === "issueLinks.threadsForIssues") {
          threadReads += 1;
          if (threadReads > failedThreadReads) return send.call(this, data);
          return reply(failure({ _tag: "IssueLinkError", detail: "The links database is busy." }));
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
                  entries: answer.issues.map((number) => ({
                    ...repository,
                    number,
                    title: `Open Issue ${number}`,
                    url: `https://github.com/example/demo/issues/${number}`,
                    state: "open",
                    author: null,
                    labels: [],
                    milestone: null,
                    commentCount: 0,
                    createdAt: "2026-10-01T00:00:00.000Z",
                    updatedAt: "2026-10-01T00:00:00.000Z",
                    parent: null,
                    subIssues: [],
                    subIssueCount: 0,
                    openBlockerCount: 0,
                    closingPullRequests: [],
                  })),
                  nextCursors: answer.next === undefined ? {} : { [SEARCH]: answer.next },
                },
              },
        );
      };
    },
    { answersByPage: plan, failedThreadReads },
  );
}
