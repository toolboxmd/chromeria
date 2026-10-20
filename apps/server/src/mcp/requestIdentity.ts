import { interactiveAuthority } from "../peer/PeerAuthorityRecord.ts";
import type { McpInvocationScope } from "./McpInvocationContext.ts";

export const threadRequestNamespace = (environmentId: string, threadId: string): string =>
  `thread:${environmentId}:${threadId}`;

/** Credentials authenticate a job; renewing one must not rename its commands. */
export const requestNamespace = (scope: McpInvocationScope): string => {
  const owner = interactiveAuthority(scope.authority)?.envelope.requestOwner;
  if (scope.client?.linked === true && owner !== undefined) return owner.namespace;
  return scope.thread !== undefined
    ? threadRequestNamespace(scope.environmentId, scope.thread.threadId)
    : scope.requestNamespace;
};

/** Keep the generation separate from the key so replacement denies rather than duplicates. */
export const requestGeneration = (scope: McpInvocationScope): string =>
  interactiveAuthority(scope.authority)?.envelope.requestOwner?.generation ??
  interactiveAuthority(scope.authority)?.peer.generation ??
  (scope.authority !== undefined && "generation" in scope.authority
    ? scope.authority.generation
    : undefined) ??
  (scope.client?.linked === true ? `legacy:${scope.client.sessionId}` : "local");
