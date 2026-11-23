import { useAtomValue } from "@effect/atom-react";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { EnvironmentThreadStructure } from "@t3tools/client-runtime/state/shell";
import {
  CHILD_REQUEST_NOTICE_LABELS,
  type ChildRequestNoticeState,
  resolveChildRequestNoticeState,
} from "@t3tools/client-runtime/user-message";
import type { ChildRequestNoticeAttribution, EnvironmentId } from "@t3tools/contracts";
import {
  CheckIcon,
  CircleSlashIcon,
  HourglassIcon,
  type LucideIcon,
  ShieldQuestionMarkIcon,
  XIcon,
} from "lucide-react";
import { Atom } from "effect/reactivity";
import { useMessages } from "gt-react";

import { useQuietThreadShell } from "../../state/entities";
import { environmentThreadDetails } from "../../state/threads";

const NO_THREAD_ATOM = Atom.make<EnvironmentThreadStructure | null>(null).pipe(
  Atom.withLabel("web-child-request-notice:none"),
);

const STATE_ICONS = {
  waiting: HourglassIcon,
  approved: CheckIcon,
  answered: CheckIcon,
  declined: XIcon,
  closed: CircleSlashIcon,
  settled: CheckIcon,
  unknown: ShieldQuestionMarkIcon,
} satisfies Record<ChildRequestNoticeState, LucideIcon>;

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
  const state = useAtomValue(
    childRef === null || waiting
      ? NO_THREAD_ATOM
      : environmentThreadDetails.quietThreadAtom(childRef),
    (thread) =>
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
    title: shell?.title.trim() || null,
    state,
    summary: m(CHILD_REQUEST_NOTICE_LABELS[childRequest.kind][state]),
    icon: STATE_ICONS[state],
  };
}
