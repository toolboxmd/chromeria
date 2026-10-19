import { RemotePendingRequest } from "./RemotePendingRequest";
import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, OrchestrationV2Subagent, PeerTaskResource } from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
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
  useEffect(() => {
    if (observedSequence !== undefined) refresh();
  }, [observedSequence, refresh]);
  const view = AsyncResult.isSuccess(result) ? result.value : null;
  const destinationLabel = props.task.remoteChild?.label ?? "Remote environment";
  return (
    <Sheet
      title={view?.snapshot.title ?? props.task.title ?? "Remote delegated task"}
      onClose={props.onClose}
    >
      <ScrollView
        contentInsetAdjustmentBehavior="automatic"
        contentContainerClassName="gap-4 px-5 pb-6"
      >
        <Text>
          {resource ? `via ${destinationLabel}` : destinationLabel} ·{" "}
          {props.task.remoteChild?.availability ?? "stale"} ·{" "}
          {view?.snapshot.workState.replaceAll("_", " ") ?? "Loading"}
        </Text>
        {props.task.remoteChild?.stop ? (
          <Text>Stop: {props.task.remoteChild.stop.state.replaceAll("_", " ")}</Text>
        ) : null}
        {AsyncResult.isFailure(result) ? (
          <Text accessibilityRole="alert" className="text-danger">
            Remote work is unavailable. The last observation may be stale. Refresh to retry.
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
                      : `via ${destinationLabel}`
                  }
                  request={request}
                  onResolved={refresh}
                />
              ))}
          </>
        ) : null}
        <View className="gap-2 rounded-xl border border-border-subtle bg-grouped-card p-4">
          <Text className="font-t3-bold">Original result</Text>
          <Text selectable>{props.task.result ?? "Pending"}</Text>
        </View>
        {props.task.remoteChild?.latestResult ? (
          <Text selectable>
            Latest received result ({props.task.remoteChild.latestResult.status}):{" "}
            {props.task.remoteChild.latestResult.text}
          </Text>
        ) : null}
        {view ? (
          <>
            <Text>
              {view.snapshot.pendingChildren} pending children · {view.snapshot.queuedRuns} queued
              runs · {view.snapshot.requestCount} pending requests
              {view.snapshot.recoveryWaiting ? " · Waiting for recovery" : ""}
              {view.snapshot.incomplete ? " · Partial observation" : ""}
            </Text>
            <RequestActionButton
              tone="secondary"
              label={showDetails ? "Hide task details" : "Task details"}
              accessibilityState={{ expanded: showDetails }}
              onPress={() => setShowDetails(!showDetails)}
            />
            {showDetails ? (
              <View className="gap-1">
                <Text selectable>Environment: {view.environmentId}</Text>
                <Text selectable>Thread: {view.threadId}</Text>
                <Text>Observed: {view.snapshot.observedAt}</Text>
                {props.task.remoteChild?.latestResult ? (
                  <Text selectable>Latest run: {props.task.remoteChild.latestResult.runId}</Text>
                ) : null}
              </View>
            ) : null}
            {resource ? (
              <RequestActionButton
                tone="secondary"
                label="Back to delegated thread"
                onPress={() => {
                  setResource(undefined);
                  setAfter(0);
                  setTextOffset(0);
                }}
              />
            ) : null}
            {view.childrenTruncated ? <Text>Showing the first 100 nested tasks.</Text> : null}
            {view.children.map((child) => (
              <View
                key={child.taskId}
                className="gap-3 rounded-xl border border-border-subtle bg-grouped-card p-4"
              >
                <Text>
                  {child.title ?? "Nested task"} · {child.status}
                </Text>
                {child.threadId !== null && child.environmentId === view.environmentId ? (
                  <RequestActionButton
                    tone="secondary"
                    label="Open nested work"
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
                label="Next messages"
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
            label="Refresh"
            onPress={() => {
              setAfter(0);
              setTextOffset(0);
              refresh();
            }}
          />
          <RequestActionButton
            tone="danger"
            label="Stop remote work"
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
