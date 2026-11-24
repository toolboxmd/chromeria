import { RemotePendingRequest } from "./RemotePendingRequest";
import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, OrchestrationV2Subagent, PeerTaskResource } from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { T, useGT, Var } from "gt-react-native";
import { useEffect, useState } from "react";
import { ScrollView, View } from "react-native";
import * as Crypto from "expo-crypto";
import { Sheet } from "../../components/ui/Sheet";
import { RequestActionButton } from "./RequestActionButton";
import { AppText as Text } from "../../components/ui/AppText";
import { remoteTasks } from "../../state/remote-tasks";
import { useAtomCommand } from "../../state/use-atom-command";

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
  const viaDestination = gt("via {label}", { label: destinationLabel });
  return (
    <Sheet
      title={view?.snapshot.title ?? props.task.title ?? gt("Remote delegated task")}
      onClose={props.onClose}
    >
      <ScrollView
        contentInsetAdjustmentBehavior="automatic"
        contentContainerClassName="gap-4 px-5 pb-6"
      >
        <Text>
          {resource ? viaDestination : destinationLabel} ·{" "}
          {props.task.remoteChild?.availability ?? gt("stale", { $context: "remote availability" })}{" "}
          · {view?.snapshot.workState.replaceAll("_", " ") ?? gt("Loading")}
        </Text>
        {props.task.remoteChild?.stop ? (
          <Text>
            <T>
              Stop: <Var>{props.task.remoteChild.stop.state.replaceAll("_", " ")}</Var>
            </T>
          </Text>
        ) : null}
        {AsyncResult.isFailure(result) ? (
          <Text accessibilityRole="alert" className="text-danger">
            <T>Remote work is unavailable. The last observation may be stale. Refresh to retry.</T>
          </Text>
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
                      : viaDestination
                  }
                  request={request}
                  onResolved={refresh}
                />
              ))}
          </>
        ) : null}
        <View className="gap-2 rounded-xl border border-border-subtle bg-grouped-card p-4">
          <Text className="font-t3-bold">
            <T>Original result</T>
          </Text>
          <Text selectable>
            {props.task.result ?? gt("Pending", { $context: "result not received yet" })}
          </Text>
        </View>
        {props.task.remoteChild?.latestResult ? (
          <Text selectable>
            <T>
              Latest received result (<Var>{props.task.remoteChild.latestResult.status}</Var>):{" "}
              <Var>{props.task.remoteChild.latestResult.text}</Var>
            </T>
          </Text>
        ) : null}
        {view ? (
          <>
            <Text>
              {gt(
                "{pending} pending children · {queued} queued runs · {requests} pending requests",
                {
                  pending: String(view.snapshot.pendingChildren),
                  queued: String(view.snapshot.queuedRuns),
                  requests: String(view.snapshot.requestCount),
                },
              )}
              {view.snapshot.recoveryWaiting ? ` · ${gt("Waiting for recovery")}` : ""}
              {view.snapshot.incomplete ? ` · ${gt("Partial observation")}` : ""}
            </Text>
            <RequestActionButton
              tone="secondary"
              label={showDetails ? gt("Hide task details") : gt("Task details")}
              accessibilityState={{ expanded: showDetails }}
              onPress={() => setShowDetails(!showDetails)}
            />
            {showDetails ? (
              <View className="gap-1">
                <Text selectable>
                  <T>
                    Environment: <Var>{view.environmentId}</Var>
                  </T>
                </Text>
                <Text selectable>
                  <T>
                    Thread: <Var>{view.threadId}</Var>
                  </T>
                </Text>
                <Text>
                  <T>
                    Observed: <Var>{view.snapshot.observedAt}</Var>
                  </T>
                </Text>
                {props.task.remoteChild?.latestResult ? (
                  <Text selectable>
                    <T>
                      Latest run: <Var>{props.task.remoteChild.latestResult.runId}</Var>
                    </T>
                  </Text>
                ) : null}
              </View>
            ) : null}
            {resource ? (
              <RequestActionButton
                tone="secondary"
                label={gt("Back to delegated thread")}
                onPress={() => {
                  setResource(undefined);
                  setAfter(0);
                  setTextOffset(0);
                }}
              />
            ) : null}
            {view.childrenTruncated ? (
              <Text>
                <T>Showing the first 100 nested tasks.</T>
              </Text>
            ) : null}
            {view.children.map((child) => (
              <View
                key={child.taskId}
                className="gap-3 rounded-xl border border-border-subtle bg-grouped-card p-4"
              >
                <Text>
                  {child.title ?? gt("Nested task")} · {child.status}
                </Text>
                {child.threadId !== null && child.environmentId === view.environmentId ? (
                  <RequestActionButton
                    tone="secondary"
                    label={gt("Open nested work")}
                    onPress={() => {
                      setResource({
                        environmentId: child.environmentId,
                        threadId: child.threadId!,
                      });
                      setAfter(0);
                      setTextOffset(0);
                    }}
                  />
                ) : null}
              </View>
            ))}
            {view.messages.map((message) => (
              <View key={message.id} className="gap-2">
                <Text>{message.role}</Text>
                <Text selectable>{message.text}</Text>
              </View>
            ))}
            {view.nextCursor !== null ? (
              <RequestActionButton
                tone="secondary"
                label={gt("Next messages")}
                onPress={() => {
                  setAfter(view.nextCursor!);
                  setTextOffset(view.nextTextOffset ?? 0);
                }}
              />
            ) : null}
          </>
        ) : null}
        <View className="gap-3 pt-2">
          <RequestActionButton
            tone="secondary"
            label={gt("Refresh")}
            onPress={() => {
              setAfter(0);
              setTextOffset(0);
              refresh();
            }}
          />
          <RequestActionButton
            tone="danger"
            label={gt("Stop remote work")}
            disabled={!mayStop}
            onPress={() =>
              void stop({
                environmentId: props.environmentId,
                input: {
                  parentThreadId: props.task.threadId,
                  taskId: props.task.id,
                  clientRequestId: Crypto.randomUUID(),
                },
              })
            }
          />
        </View>
      </ScrollView>
    </Sheet>
  );
}
