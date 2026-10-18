import type { McpInvocationScope } from "./McpInvocationContext.ts";

export const threadRequestNamespace = (environmentId: string, threadId: string): string =>
  `thread:${environmentId}:${threadId}`;

/** Credentials authenticate a job; renewing one must not rename its commands. */
export const requestNamespace = (scope: McpInvocationScope): string =>
  scope.client?.linked === true && scope.authority?.envelope.requestOwner !== undefined
    ? scope.authority.envelope.requestOwner.namespace
    : scope.thread !== undefined
      ? threadRequestNamespace(scope.environmentId, scope.thread.threadId)
      : scope.requestNamespace;

/** Keep the generation separate from the key so replacement denies rather than duplicates. */
export const requestGeneration = (scope: McpInvocationScope): string =>
  scope.authority?.envelope.requestOwner?.generation ??
  scope.authority?.peer.generation ??
  (scope.client?.linked === true ? `legacy:${scope.client.sessionId}` : "local");
