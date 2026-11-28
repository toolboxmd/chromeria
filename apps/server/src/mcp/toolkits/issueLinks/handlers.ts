import * as Option from "effect/Option";
import { CurrentRequest, McpRequestReceipts } from "../../McpRequestReceipts.ts";
import * as Effect from "effect/Effect";

import { issueUrlFor, issueKeysEqual } from "@t3tools/contracts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import {
  updateAgentLinkPurpose,
  resumeLinkPurpose,
  resolveKeyedLinkTarget,
  withAgentLinkLock,
} from "../../agentLinkPurpose.ts";
import * as IssueLinks from "../../../issueLinks/IssueLinks.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { IssueLinksToolkit } from "./tools.ts";

// Issue links ride on the pull-request capability: both are the thread's source-control links.
// Every tool acts on the calling thread only, which `McpToolAccess` requires to be a T3 thread.
const callerThread = McpInvocationContext.requireMcpCapability("pull-requests").pipe(
  Effect.flatMap((scope) => McpInvocationContext.requireThreadScope(scope, "This tool")),
  Effect.map((scope) => scope.thread.threadId),
);

// A keyed retry returns the original SQL result even if a later call removed the link.
const replayingIssueMutation = Effect.gen(function* () {
  const request = yield* CurrentRequest;
  if (
    request === undefined ||
    (yield* McpInvocationContext.McpInvocationContext).client !== undefined
  )
    return false;
  const receipts = yield* Effect.serviceOption(McpRequestReceipts);
  return Option.isSome(receipts) && Option.isSome(yield* receipts.value.replay(request));
});

const make = Effect.gen(function* () {
  const links = yield* IssueLinks.IssueLinks;
  return {
    link_issue: McpToolAccess.actsAsCaller((target) =>
      withAgentLinkLock(
        undefined,
        Effect.gen(function* () {
          const threadId = yield* callerThread;
          const replay = yield* replayingIssueMutation;
          const phase = yield* resumeLinkPurpose(threadId, replay);
          const boundTarget =
            target.url === undefined && (yield* CurrentRequest) !== undefined
              ? {
                  url: (yield* resolveKeyedLinkTarget(
                    threadId,
                    links.resolveTarget(threadId, target),
                  )).url,
                }
              : target;
          const { link, alreadyLinked } = yield* links.link({
            threadId,
            target: boundTarget,
            source: "agent",
          });
          if (
            phase.pending &&
            (!replay ||
              (yield* links.forThread(threadId)).some((item) => issueKeysEqual(item, link)))
          )
            yield* updateAgentLinkPurpose(yield* ThreadManagementService, threadId, {
              type: "link",
              urls: [issueUrlFor(link)],
            });
          if (phase.pending) yield* phase.finish;
          return { ...link, alreadyLinked };
        }),
      ),
    ),
    unlink_issue: McpToolAccess.actsAsCaller((target) =>
      withAgentLinkLock(
        undefined,
        Effect.gen(function* () {
          const threadId = yield* callerThread;
          const { url, ...issue } = yield* resolveKeyedLinkTarget(
            threadId,
            links.resolveTarget(threadId, target),
          );
          const replay = yield* replayingIssueMutation;
          const phase = yield* resumeLinkPurpose(threadId, replay);
          const { wasLinked } = yield* links.unlink({ threadId, issue });
          if (
            phase.pending &&
            (!replay ||
              !(yield* links.forThread(threadId)).some((item) => issueKeysEqual(item, issue)))
          )
            yield* updateAgentLinkPurpose(yield* ThreadManagementService, threadId, {
              type: "unlink",
              urls: [url],
            });
          if (phase.pending) yield* phase.finish;
          return { ...issue, wasLinked };
        }),
      ),
    ),
    list_thread_issues: McpToolAccess.readsAsCaller(() =>
      Effect.gen(function* () {
        const threadId = yield* callerThread;
        return { issues: yield* links.forThread(threadId) };
      }),
    ),
  } satisfies McpToolAccess.Handlers<typeof IssueLinksToolkit.tools>;
});

export const layer = McpToolAccess.toLayer(IssueLinksToolkit, make);
