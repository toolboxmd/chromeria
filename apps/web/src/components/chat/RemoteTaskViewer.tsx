import { RemotePendingRequest } from "./RemotePendingRequest";
import { randomUUID } from "../../lib/utils";
import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, OrchestrationV2Subagent, PeerTaskResource } from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { T, useGT, Var } from "gt-react";
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
  const gt = useGT();
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
  const destinationLabel = props.task.remoteChild?.label ?? gt("Remote environment");
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
            {view?.snapshot.title ?? props.task.title ?? gt("Remote delegated task")}
          </DialogTitle>
          <p>
            {resource ? gt("via {label}", { label: destinationLabel }) : destinationLabel} ·{" "}
            {props.task.remoteChild?.availability ??
              gt("stale", { $context: "remote availability" })}{" "}
            · {view?.snapshot.workState.replaceAll("_", " ") ?? gt("Loading")}
          </p>
          {props.task.remoteChild?.stop ? (
            <p>
              <T>
                Stop: <Var>{props.task.remoteChild.stop.state.replaceAll("_", " ")}</Var>
              </T>
            </p>
          ) : null}
          {AsyncResult.isFailure(result) ? (
            <p role="alert">
              <T>
                Remote work is unavailable. The last observation may be stale. Refresh to retry.
              </T>
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
                        : gt("via {label}", { label: destinationLabel })
                    }
                    request={request}
                    onResolved={refresh}
                  />
                ))}
            </>
          ) : null}
          <div className="max-h-48 overflow-y-auto whitespace-pre-wrap break-words">
            <p>
              <T>
                Original result: <Var>{props.task.result ?? gt("Pending")}</Var>
              </T>
            </p>
            {props.task.remoteChild?.latestResult ? (
              <p>
                <T>
                  Latest received result (<Var>{props.task.remoteChild.latestResult.status}</Var>):{" "}
                  <Var>{props.task.remoteChild.latestResult.text}</Var>
                </T>
              </p>
            ) : null}
          </div>
          {view ? (
            <>
              <p>
                {gt(
                  "{children, plural, one {{children} pending children} other {{children} pending children}} · {runs, plural, one {{runs} queued runs} other {{runs} queued runs}} · {requests, plural, one {{requests} pending requests} other {{requests} pending requests}}",
                  {
                    children: view.snapshot.pendingChildren,
                    runs: view.snapshot.queuedRuns,
                    requests: view.snapshot.requestCount,
                  },
                )}
                {view.snapshot.recoveryWaiting ? ` · ${gt("Waiting for recovery")}` : ""}
                {view.snapshot.incomplete ? ` · ${gt("Partial observation")}` : ""}
              </p>
              <Button
                variant="ghost"
                size="sm"
                aria-expanded={showDetails}
                onClick={() => setShowDetails(!showDetails)}
              >
                {showDetails ? gt("Hide task details") : gt("Task details")}
              </Button>
              {showDetails ? (
                <div className="break-all text-xs text-muted-foreground">
                  <p>
                    <T>
                      Environment: <Var>{view.environmentId}</Var>
                    </T>
                  </p>
                  <p>
                    <T>
                      Thread: <Var>{view.threadId}</Var>
                    </T>
                  </p>
                  <p>
                    <T>
                      Observed: <Var>{view.snapshot.observedAt}</Var>
                    </T>
                  </p>
                  {props.task.remoteChild?.latestResult ? (
                    <p>
                      <T>
                        Latest run: <Var>{props.task.remoteChild.latestResult.runId}</Var>
                      </T>
                    </p>
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
                  <T>Back to delegated thread</T>
                </Button>
              ) : null}
              {view.childrenTruncated ? (
                <p>
                  <T>Showing the first 100 nested tasks.</T>
                </p>
              ) : null}
              {view.children.map((child) => (
                <div key={child.taskId}>
                  <span>
                    {child.title ?? gt("Nested task")} · {child.status}
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
                      <T>Open nested work</T>
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
                  <T>Next messages</T>
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
              <T>Refresh</T>
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
              <T>Stop remote work</T>
            </Button>
          </div>
        </div>
      </DialogPopup>
    </Dialog>
  );
}
