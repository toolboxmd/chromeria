import { useState } from "react";
import type {
  EnvironmentId,
  OrchestrationV2Subagent,
  ScopedThreadRef,
  ThreadId,
} from "@t3tools/contracts";
import { hasSourceRemoteTaskView } from "@t3tools/client-runtime/state/remote-tasks";
import {
  subagentChildTarget,
  type SubagentChildTarget,
} from "@t3tools/client-runtime/state/thread-relationships";
import { useServerConfigs } from "../../state/entities";
import { RemoteTaskViewer } from "./RemoteTaskViewer";

/** All task opens select the source viewer before considering destination profiles. */
export function useSubagentOpen(
  parentRef: ScopedThreadRef,
  agent: OrchestrationV2Subagent | null,
  onOpenThread: (threadId: ThreadId, environmentId?: EnvironmentId) => void,
  fallback: SubagentChildTarget | null = null,
) {
  const [viewing, setViewing] = useState(false);
  const configs = useServerConfigs();
  const source = hasSourceRemoteTaskView(agent ?? undefined);
  const target = agent === null ? fallback : subagentChildTarget(agent);
  const canOpen =
    source ||
    (target !== null && (target.environmentId === null || configs.has(target.environmentId)));
  return {
    canOpen,
    open: () => {
      if (source) setViewing(true);
      else if (canOpen && target !== null) {
        if (target.environmentId === null) onOpenThread(target.threadId);
        else onOpenThread(target.threadId, target.environmentId);
      }
    },
    viewer:
      viewing && source && agent !== null ? (
        <RemoteTaskViewer
          environmentId={parentRef.environmentId}
          task={agent}
          onClose={() => setViewing(false)}
        />
      ) : null,
  };
}
