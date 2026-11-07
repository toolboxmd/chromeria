import { RemotePendingRequest } from "./RemotePendingRequest";
import { randomUUID } from "../../lib/utils";
import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, OrchestrationV2Subagent, PeerTaskResource } from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { useEffect, useState } from "react";
import { remoteTasks } from "../../state/remoteTasks";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { Dialog, DialogPopup, DialogTitle } from "../ui/dialog";

export function RemoteTaskViewer(props: {
  environmentId: EnvironmentId;
  task: Pick<OrchestrationV2Subagent, "id" | "threadId" | "title" | "result" | "remoteChild">;
  onClose: () => void;
}) {
  const [after, setAfter] = useState(0);
  const [showDetails, setShowDetails] = useState(false);
  const [resource, setResource] = useState<PeerTaskResource | undefined>(undefined);
  const [textOffset, setTextOffset] = useState(0);
  const query = remoteTasks.view({
    environmentId: props.environmentId,
    input: {
      parentThreadId: props.task.threadId,
      taskId: props.task.id,
      after,
      textOffset,
      ...(resource === undefined ? {} : { resource }),
    },
  });
  const result = useAtomValue(query);
  const refresh = useAtomRefresh(query);
  const stop = useAtomCommand(remoteTasks.stop);
  const mayStop = useAtomValue(remoteTasks.stop.permissionAtom(props.environmentId));
  const observedSequence = props.task.remoteChild?.snapshot?.sequence;
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Refresh the remote RPC read when its persisted observation sequence advances.
  useEffect(() => {
    if (observedSequence !== undefined) refresh();
  }, [observedSequence, refresh]);
  const view = AsyncResult.isSuccess(result) ? result.value : null;
  const destinationLabel = props.task.remoteChild?.label ?? "Remote environment";
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) props.onClose();
      }}
    >
      <DialogPopup>
        <div className="max-h-[75vh] space-y-4 overflow-y-auto">
          <DialogTitle>
            {view?.snapshot.title ?? props.task.title ?? "Remote delegated task"}
          </DialogTitle>
          <p>
            {resource ? `via ${destinationLabel}` : destinationLabel} ·{" "}
            {props.task.remoteChild?.availability ?? "stale"} ·{" "}
            {view?.snapshot.workState.replaceAll("_", " ") ?? "Loading"}
          </p>
          {props.task.remoteChild?.stop ? (
            <p>Stop: {props.task.remoteChild.stop.state.replaceAll("_", " ")}</p>
          ) : null}
          {AsyncResult.isFailure(result) ? (
            <p role="alert">
              Remote work is unavailable. The last observation may be stale. Refresh to retry.
            </p>
          ) : null}
          {view ? (
            <>
              {view.snapshot.requests
                .filter(
                  (request) =>
                    request.kind !== "auth_refresh" && request.kind !== "dynamic_tool_call",
                )
                .map((request) => (
                  <RemotePendingRequest
                    key={`${request.environmentId}:${request.threadId}:${request.requestId}:${request.revision}`}
                    environmentId={props.environmentId}
                    task={props.task}
                    destinationLabel={
                      request.environmentId === props.task.remoteChild?.environmentId
                        ? destinationLabel
                        : `via ${destinationLabel}`
                    }
                    request={request}
                    onResolved={refresh}
                  />
                ))}
            </>
          ) : null}
          <div className="max-h-48 overflow-y-auto whitespace-pre-wrap break-words">
            <p>Original result: {props.task.result ?? "Pending"}</p>
            {props.task.remoteChild?.latestResult ? (
              <p>
                Latest received result ({props.task.remoteChild.latestResult.status}):{" "}
                {props.task.remoteChild.latestResult.text}
              </p>
            ) : null}
          </div>
          {view ? (
            <>
              <p>
                {view.snapshot.pendingChildren} pending children · {view.snapshot.queuedRuns} queued
                runs · {view.snapshot.requestCount} pending requests
                {view.snapshot.recoveryWaiting ? " · Waiting for recovery" : ""}
                {view.snapshot.incomplete ? " · Partial observation" : ""}
              </p>
              <Button
                variant="ghost"
                size="sm"
                aria-expanded={showDetails}
                onClick={() => setShowDetails(!showDetails)}
              >
                {showDetails ? "Hide task details" : "Task details"}
              </Button>
              {showDetails ? (
                <div className="break-all text-xs text-muted-foreground">
                  <p>Environment: {view.environmentId}</p>
                  <p>Thread: {view.threadId}</p>
                  <p>Observed: {view.snapshot.observedAt}</p>
                  {props.task.remoteChild?.latestResult ? (
                    <p>Latest run: {props.task.remoteChild.latestResult.runId}</p>
                  ) : null}
                </div>
              ) : null}
              {resource ? (
                <Button
                  variant="outline"
                  onClick={() => {
                    setResource(undefined);
                    setAfter(0);
                    setTextOffset(0);
                  }}
                >
                  Back to delegated thread
                </Button>
              ) : null}
              {view.childrenTruncated ? <p>Showing the first 100 nested tasks.</p> : null}
              {view.children.map((child) => (
                <div key={child.taskId}>
                  <span>
                    {child.title ?? "Nested task"} · {child.status}
                  </span>
                  {child.threadId !== null && child.environmentId === view.environmentId ? (
                    <Button
                      variant="outline"
                      onClick={() => {
                        setResource({
                          environmentId: child.environmentId,
                          threadId: child.threadId!,
                        });
                        setAfter(0);
                        setTextOffset(0);
                      }}
                    >
                      Open nested work
                    </Button>
                  ) : null}
                </div>
              ))}
              <div className="max-h-96 overflow-y-auto whitespace-pre-wrap">
                {view.messages.map((message) => (
                  <div key={message.id} className="mb-4">
                    <strong>{message.role}</strong>
                    <p>{message.text}</p>
                  </div>
                ))}
              </div>
              {view.nextCursor !== null ? (
                <Button
                  variant="outline"
                  onClick={() => {
                    setAfter(view.nextCursor!);
                    setTextOffset(view.nextTextOffset ?? 0);
                  }}
                >
                  Next messages
                </Button>
              ) : null}
            </>
          ) : null}
          <div className="mt-4 flex gap-2">
            <Button
              variant="outline"
              onClick={() => {
                setAfter(0);
                setTextOffset(0);
                refresh();
              }}
            >
              Refresh
            </Button>
            <Button
              variant="outline"
              disabled={!mayStop}
              onClick={() =>
                void stop({
                  environmentId: props.environmentId,
                  input: {
                    parentThreadId: props.task.threadId,
                    taskId: props.task.id,
                    clientRequestId: randomUUID(),
                  },
                })
              }
            >
              Stop remote work
            </Button>
          </div>
        </div>
      </DialogPopup>
    </Dialog>
  );
}
