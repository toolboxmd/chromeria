import {
  purposeSelectionOf,
  savedPurposeIntentForScope,
  type OrchestrationV2Command,
  type PurposeSelection,
  type RepositoryIdentity,
  type ScopedThreadRef,
  type ThreadId,
  type ThreadPullRequestLink,
} from "@t3tools/contracts";
import { canonicalizeGitCafePullRequestUrl } from "@t3tools/source-control-definitions/registry";
import { threadPullRequestKeysEqual } from "./threadPullRequests.ts";

import { sourceControlClients, type ChangeRequestTerminology } from "./sourceControlClients.ts";

type WatchThreadPullRequestInput = Omit<
  Extract<OrchestrationV2Command, { type: "thread.pull-request.watch" }>,
  "type" | "commandId"
>;

const PULL_REQUEST_NUMBER_PATTERN = /^#?(\d+)$/;

export function parsePullRequestReference(input: string): string | null {
  const trimmed = input.trim();
  if (trimmed.length === 0) {
    return null;
  }

  const normalizedInput =
    sourceControlClients.definitions
      .map((definition) => definition.checkoutCommandArgument(trimmed))
      .find((argument) => argument !== null) ?? trimmed;
  if (normalizedInput.length === 0) {
    return null;
  }

  if (
    sourceControlClients.definitions.some((definition) =>
      definition.isChangeRequestReference(normalizedInput),
    )
  ) {
    return normalizedInput;
  }

  const numberMatch = PULL_REQUEST_NUMBER_PATTERN.exec(normalizedInput);
  if (numberMatch?.[1]) {
    return numberMatch[1];
  }

  return null;
}

/** The thread project's repository, so a bare number resolves to one of its pull requests. */
export function pullRequestLinkProject(identity: RepositoryIdentity | null | undefined) {
  if (!identity) return null;
  const repository =
    identity.displayName ??
    (identity.owner && identity.name ? `${identity.owner}/${identity.name}` : null);
  if (repository === null) return null;
  const definition = sourceControlClients.get(identity.provider ?? "unknown");
  const host = sourceControlClients.changeRequestHost(identity, definition.kind);
  return {
    host,
    repository,
    terminology: definition.changeRequest,
    webUrl: (number: number) =>
      definition.changeRequestUrl({
        host,
        repository,
        number,
        remoteUrl: identity.locator.remoteUrl,
        webUrl: identity.webUrl,
      }),
  };
}

interface ResolvedLink {
  readonly host: string;
  readonly repository: string;
  readonly number: number;
  readonly url: string;
}

export function resolveLinkPullRequestInput(input: {
  readonly reference: string;
  readonly project: {
    readonly host: string;
    readonly repository: string;
    readonly terminology: ChangeRequestTerminology;
    readonly webUrl: (number: number) => string | null;
  } | null;
  readonly hasProject: (reference: ResolvedLink) => boolean;
}): { link: ResolvedLink } | { error: string } | null {
  const parsed =
    sourceControlClients.parseChangeRequestUrl(input.reference.trim()) !== null
      ? input.reference.trim()
      : parsePullRequestReference(input.reference);
  if (parsed === null) return null;
  const url = sourceControlClients.parseChangeRequestUrl(parsed)?.link;
  if (url !== undefined) {
    if (!input.hasProject({ ...url, url: parsed })) {
      return { error: `No project in this environment can read ${url.host}/${url.repository}.` };
    }
    return {
      link: {
        host: url.host,
        repository: url.repository,
        number: url.number,
        url: canonicalizeGitCafePullRequestUrl(parsed),
      },
    };
  }
  const number = Number(parsed);
  if (!Number.isSafeInteger(number) || number < 1) return null;
  if (input.project === null) {
    return {
      error:
        "Paste the full pull request URL; this thread has no repository to resolve the number against.",
    };
  }
  const webUrl = input.project.webUrl(number);
  const webReference =
    webUrl === null ? undefined : sourceControlClients.parseChangeRequestUrl(webUrl)?.link;
  if (webUrl === null || webReference === undefined) {
    return {
      error: `Paste a full URL; this project's host has no known ${input.project.terminology.singular} URL.`,
    };
  }
  return {
    link: {
      host: webReference.host,
      repository: webReference.repository,
      number: webReference.number,
      url: webUrl,
    },
  };
}

export function threadPullRequestWatchInput(input: {
  readonly threadId: ThreadId;
  readonly reference: ThreadPullRequestLink;
  readonly ownLinks: ReadonlyArray<ThreadPullRequestLink>;
  readonly watching: boolean;
}): WatchThreadPullRequestInput {
  return {
    threadId: input.threadId,
    host: input.reference.host,
    repository: input.reference.repository,
    number: input.reference.number,
    watching: input.watching,
    ...(input.watching &&
    !input.ownLinks.some((link) => threadPullRequestKeysEqual(link, input.reference))
      ? { link: { url: input.reference.url, source: "manual" } }
      : {}),
  };
}

export function createThreadPullRequestWatchAction<Result>(input: {
  readonly threadRef: ScopedThreadRef;
  readonly ownLinks: ReadonlyArray<ThreadPullRequestLink>;
  /** Present when the environment advertises purpose v1: the thread's current selection. */
  readonly purpose?: { readonly selection: PurposeSelection | undefined } | undefined;
  readonly watch: (command: {
    readonly environmentId: ScopedThreadRef["environmentId"];
    readonly input: WatchThreadPullRequestInput;
  }) => Result;
}) {
  return (reference: ThreadPullRequestLink, watching: boolean) =>
    input.watch({
      environmentId: input.threadRef.environmentId,
      input: {
        ...threadPullRequestWatchInput({
          threadId: input.threadRef.threadId,
          ownLinks: input.ownLinks,
          reference,
          watching,
        }),
        // A new watch saves the thread's chosen purpose, as a schedule does.
        ...(watching && input.purpose !== undefined
          ? {
              purposeIntent: savedPurposeIntentForScope(
                purposeSelectionOf({ purposeSelection: input.purpose.selection }).scope,
              ),
            }
          : {}),
      },
    });
}
