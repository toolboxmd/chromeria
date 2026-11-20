import { createRemoteTaskState } from "@t3tools/client-runtime/state/remote-tasks";
import { connectionAtomRuntime } from "../connection/runtime";
export const remoteTasks = createRemoteTaskState(connectionAtomRuntime);
