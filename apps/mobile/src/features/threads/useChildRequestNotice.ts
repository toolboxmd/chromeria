import { useAtomValue } from "@effect/atom-react";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { EnvironmentThreadStructure } from "@t3tools/client-runtime/state/shell";
import {
  CHILD_REQUEST_NOTICE_LABELS,
  type ChildRequestNoticeState,
  resolveChildRequestNoticeState,
} from "@t3tools/client-runtime/user-message";
import type { ChildRequestNoticeAttribution, EnvironmentId } from "@t3tools/contracts";
import { Atom } from "effect/reactivity";
import { useMessages } from "gt-react-native";

import type { AppSymbolName } from "../../components/ui/AppSymbol";
import { useQuietThreadShell } from "../../state/entities";
import { environmentThreadDetails } from "../../state/threads";

const NO_THREAD_ATOM = Atom.make<EnvironmentThreadStructure | null>(null).pipe(
  Atom.withLabel("mobile-child-request-notice:none"),
);

const STATE_SYMBOLS = {
  waiting: "person.badge.clock",
  approved: "checkmark.circle",
  answered: "checkmark.circle",
  declined: "xmark.circle.fill",
  closed: "slash.circle",
  settled: "checkmark.circle",
  unknown: "questionmark.bubble",
} satisfies Record<ChildRequestNoticeState, AppSymbolName>;

/** The one-line notice for a descendant's relayed request: the child's title and where it stands. */
export function useChildRequestNotice(
  environmentId: EnvironmentId,
  childRequest: ChildRequestNoticeAttribution | undefined,
) {
  const m = useMessages();
  const childRef =
    childRequest === undefined ? null : scopeThreadRef(environmentId, childRequest.childThreadId);
  const shell = useQuietThreadShell(childRef);
  const waiting =
    childRequest !== undefined &&
    shell?.source.pendingRuntimeRequest?.id === childRequest.requestId;
  // Only a settled request needs the child's detail, for its outcome.
  const detailAtom =
    childRef === null || waiting
      ? NO_THREAD_ATOM
      : environmentThreadDetails.quietThreadAtom(childRef);
  // The shell drops an idle child whose family is not loaded; its detail still names it.
  const detailTitle = useAtomValue(detailAtom, (thread) => thread?.projection.thread.title ?? null);
  const state = useAtomValue(detailAtom, (thread) =>
    childRequest === undefined
      ? null
      : resolveChildRequestNoticeState({
          requestId: childRequest.requestId,
          pendingRuntimeRequest: shell?.source.pendingRuntimeRequest,
          request:
            thread === null
              ? undefined
              : (thread.projection.runtimeRequests.find(
                  (request) => request.id === childRequest.requestId,
                ) ?? null),
        }),
  );
  if (childRequest === undefined || state === null) return null;
  return {
    childRef: childRef!,
    title: shell?.title.trim() || detailTitle?.trim() || null,
    state,
    summary: m(CHILD_REQUEST_NOTICE_LABELS[childRequest.kind][state]),
    symbol: STATE_SYMBOLS[state],
  };
}
