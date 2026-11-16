import * as Effect from "effect/Effect";

import * as IssueLinks from "../../../issueLinks/IssueLinks.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { IssuesToolkit } from "./tools.ts";

// Issue links ride on the pull-request capability: both are the thread's source-control links.
// Every tool acts on the calling thread only, which `McpToolAccess` requires to be a T3 thread.
const callerThread = McpInvocationContext.requireMcpCapability("pull-requests").pipe(
  Effect.flatMap((scope) => McpInvocationContext.requireThreadScope(scope, "This tool")),
  Effect.map((scope) => scope.thread.threadId),
);

const make = Effect.gen(function* () {
  const links = yield* IssueLinks.IssueLinks;
  return {
    link_issue: McpToolAccess.actsAsCaller((target) =>
      Effect.gen(function* () {
        const threadId = yield* callerThread;
        const { link, alreadyLinked } = yield* links.link({ threadId, target, source: "agent" });
        return { ...link, alreadyLinked };
      }),
    ),
    unlink_issue: McpToolAccess.actsAsCaller((target) =>
      Effect.gen(function* () {
        const threadId = yield* callerThread;
        const { url: _url, ...issue } = yield* links.resolveTarget(threadId, target);
        const { wasLinked } = yield* links.unlink({ threadId, issue });
        return { ...issue, wasLinked };
      }),
    ),
    list_thread_issues: McpToolAccess.readsAsCaller(() =>
      Effect.gen(function* () {
        const threadId = yield* callerThread;
        return { issues: yield* links.forThread(threadId) };
      }),
    ),
  } satisfies McpToolAccess.Handlers<typeof IssuesToolkit.tools>;
});

export const layer = McpToolAccess.toLayer(IssuesToolkit, make);
