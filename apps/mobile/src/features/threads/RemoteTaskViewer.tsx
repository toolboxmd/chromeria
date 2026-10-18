import { RemotePendingRequest } from "./RemotePendingRequest";
import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, OrchestrationV2Subagent, PeerTaskResource } from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { useEffect, useState } from "react";
import { Modal, Pressable, ScrollView, View } from "react-native";
import * as Crypto from "expo-crypto";
import { AppText as Text } from "../../components/AppText";
import { remoteTasks } from "../../state/remote-tasks";
import { useAtomCommand } from "../../state/use-atom-command";

export function RemoteTaskViewer(props: {
  environmentId: EnvironmentId;
  task: Pick<OrchestrationV2Subagent, "id" | "threadId" | "title" | "result" | "remoteChild">;
  onClose: () => void;
}) {
  const [after, setAfter] = useState(0);
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
  return (
    <Modal
      visible
      onRequestClose={props.onClose}
      presentationStyle="pageSheet"
      animationType="slide"
    >
      <View className="flex-1 bg-background px-5 pt-10">
        <Pressable accessibilityRole="button" onPress={props.onClose}>
          <Text>Close remote task</Text>
        </Pressable>
        <ScrollView className="mt-4 flex-1">
          <Text>{view?.snapshot.title ?? props.task.title ?? "Remote delegated task"}</Text>
          <Text>
            {props.task.remoteChild?.label} · {props.task.remoteChild?.availability ?? "stale"} ·{" "}
            {view?.snapshot.workState.replaceAll("_", " ") ?? "Loading"}
          </Text>
          <Text>Original result: {props.task.result ?? "Pending"}</Text>
          {props.task.remoteChild?.latestResult ? (
            <Text>
              Latest received run {props.task.remoteChild.latestResult.runId} (
              {props.task.remoteChild.latestResult.status}):{" "}
              {props.task.remoteChild.latestResult.text}
            </Text>
          ) : null}
          {props.task.remoteChild?.stop ? (
            <Text>Stop: {props.task.remoteChild.stop.state.replaceAll("_", " ")}</Text>
          ) : null}
          {AsyncResult.isFailure(result) ? (
            <Text>
              Remote work is unavailable. The last observation may be stale. Refresh to retry.
            </Text>
          ) : null}
          {view ? (
            <>
              <Text>Observed: {view.snapshot.observedAt}</Text>
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
                    request={request}
                    onResolved={refresh}
                  />
                ))}
              <Text>
                {view.snapshot.pendingChildren} pending children · {view.snapshot.queuedRuns} queued
                runs · {view.snapshot.requestCount} pending requests
                {view.snapshot.recoveryWaiting ? " · Waiting for recovery" : ""}
                {view.snapshot.incomplete ? " · Partial observation" : ""}
              </Text>
              <Text>
                {view.environmentId} / {view.threadId}
              </Text>
              {resource ? (
                <Pressable
                  accessibilityRole="button"
                  onPress={() => {
                    setResource(undefined);
                    setAfter(0);
                    setTextOffset(0);
                  }}
                >
                  <Text>Back to delegated thread</Text>
                </Pressable>
              ) : null}
              {view.childrenTruncated ? <Text>Showing the first 100 nested tasks.</Text> : null}
              {view.children.map((child) => (
                <View key={child.taskId}>
                  <Text>
                    {child.title ?? child.taskId} · {child.status}
                  </Text>
                  {child.threadId !== null && child.environmentId === view.environmentId ? (
                    <Pressable
                      accessibilityRole="button"
                      onPress={() => {
                        setResource({
                          environmentId: child.environmentId,
                          threadId: child.threadId!,
                        });
                        setAfter(0);
                        setTextOffset(0);
                      }}
                    >
                      <Text>Open nested work</Text>
                    </Pressable>
                  ) : null}
                </View>
              ))}
              {view.messages.map((message) => (
                <View key={message.id} className="my-3">
                  <Text>{message.role}</Text>
                  <Text selectable>{message.text}</Text>
                </View>
              ))}
              {view.nextCursor !== null ? (
                <Pressable
                  accessibilityRole="button"
                  onPress={() => {
                    setAfter(view.nextCursor!);
                    setTextOffset(view.nextTextOffset ?? 0);
                  }}
                >
                  <Text>Next messages</Text>
                </Pressable>
              ) : null}
            </>
          ) : null}
        </ScrollView>
        <View className="gap-4 py-6">
          <Pressable
            accessibilityRole="button"
            onPress={() => {
              setAfter(0);
              setTextOffset(0);
              refresh();
            }}
          >
            <Text>Refresh</Text>
          </Pressable>
          <Pressable
            accessibilityRole="button"
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
          >
            <Text>Stop remote work</Text>
          </Pressable>
        </View>
      </View>
    </Modal>
  );
}
