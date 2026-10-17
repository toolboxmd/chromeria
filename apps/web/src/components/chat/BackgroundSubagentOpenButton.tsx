import {
  NodeId,
  type EnvironmentId,
  type ScopedThreadRef,
  type ThreadId,
} from "@t3tools/contracts";
import { NotificationSubagentOpenButton } from "./V2LifecycleRow";

/** The settled-parent banner resolves the owned task before any direct-thread fallback. */
export function BackgroundSubagentOpenButton(props: {
  parentRef: ScopedThreadRef | null;
  item: {
    taskId: string;
    label: string;
    childThreadId?: ThreadId | undefined;
    childEnvironmentId?: EnvironmentId | undefined;
  };
  onOpenThread: (threadId: ThreadId, environmentId?: EnvironmentId) => void;
}) {
  if (props.parentRef === null) return <>{props.item.label}</>;
  return (
    <NotificationSubagentOpenButton
      parentRef={props.parentRef}
      subagentId={NodeId.make(props.item.taskId)}
      label={props.item.label}
      fallback={
        props.item.childThreadId === undefined
          ? undefined
          : {
              threadId: props.item.childThreadId,
              environmentId: props.item.childEnvironmentId ?? null,
            }
      }
      onOpenThread={props.onOpenThread}
    />
  );
}
