import { useState } from "react";
import { useNavigate } from "@tanstack/react-router";
import type {
  EnvironmentId,
  OrchestrationV2Subagent,
  ScopedThreadRef,
  ThreadId,
} from "@t3tools/contracts";
import { hasSourceRemoteTaskView } from "@t3tools/client-runtime/state/remote-tasks";
import type { SideThreadOpenIntent } from "@t3tools/client-runtime/side-thread";
import {
  subagentChildTarget,
  type SubagentChildTarget,
} from "@t3tools/client-runtime/state/thread-relationships";
import { openLinkedThreadRead } from "../../lib/sideChatPane";
import { useServerConfigs } from "../../state/entities";
import { RemoteTaskViewer } from "./RemoteTaskViewer";

/**
 * All task opens select the source viewer before considering destination profiles. A remote
 * child on an environment this client is not connected to opens read-only through the parent's.
 */
export function useSubagentOpen(
  parentRef: ScopedThreadRef | null,
  agent: OrchestrationV2Subagent | null,
  onOpenThread: (threadId: ThreadId, environmentId?: EnvironmentId) => void,
  fallback: SubagentChildTarget | null = null,
) {
  const [viewing, setViewing] = useState(false);
  const navigate = useNavigate();
  const configs = useServerConfigs();
  const source = hasSourceRemoteTaskView(agent ?? undefined);
  const target = agent === null ? fallback : subagentChildTarget(agent);
  const direct =
    target !== null && (target.environmentId === null || configs.has(target.environmentId));
  const remote = agent?.remoteChild;
  const readOnly =
    !source &&
    !direct &&
    parentRef !== null &&
    remote?.threadId !== undefined &&
    remote.environmentId !== parentRef.environmentId
      ? { parentRef, remote, threadId: remote.threadId }
      : null;
  const canOpen = source || direct || readOnly !== null;
  const openWithIntent = (intent: SideThreadOpenIntent) => {
    if (source) setViewing(true);
    else if (direct && target !== null) {
      if (target.environmentId === null) onOpenThread(target.threadId);
      else onOpenThread(target.threadId, target.environmentId);
    } else if (readOnly !== null) {
      openLinkedThreadRead({
        panelRef: readOnly.parentRef,
        target: {
          viaEnvironmentId: readOnly.parentRef.environmentId,
          environmentId: readOnly.remote.environmentId,
          threadId: readOnly.threadId,
          label: readOnly.remote.label,
          title: agent?.title ?? null,
        },
        intent,
        navigate,
      });
    }
  };
  return {
    canOpen,
    /** Whether a click opens the read-only view, which can open in the side panel. */
    readOnly: readOnly !== null,
    open: () => openWithIntent("primary"),
    openWithIntent,
    viewer:
      viewing && source && agent !== null && parentRef !== null ? (
        <RemoteTaskViewer
          environmentId={parentRef.environmentId}
          task={agent}
          onClose={() => setViewing(false)}
        />
      ) : null,
  };
}
