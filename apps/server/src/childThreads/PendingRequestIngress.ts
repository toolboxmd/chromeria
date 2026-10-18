import { PendingRequestClientRpcGroup, WS_METHODS } from "@t3tools/contracts";
import type { AuthenticatedSession } from "../auth/EnvironmentAuth.ts";
import type { PendingRequests } from "./PendingRequestService.ts";

/** Called only after the normal client transport authenticates its session.
 * No tool argument or request body supplies the actor identity.
 */
export const clientHandlers = (
  session: AuthenticatedSession,
  requests: PendingRequests["Service"],
) =>
  PendingRequestClientRpcGroup.of({
    [WS_METHODS.remoteTaskRequestRead]: (input) => requests.fromTask(session.sessionId, input),
    [WS_METHODS.remoteTaskRequest]: (input) => requests.fromTask(session.sessionId, input),
  });
