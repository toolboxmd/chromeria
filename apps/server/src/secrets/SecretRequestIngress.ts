import { SecretRequestClientRpcGroup, WS_METHODS } from "@t3tools/contracts";
import type { AuthenticatedSession } from "../auth/EnvironmentAuth.ts";
import type { SecretRequests } from "./SecretRequests.ts";

/** Only authenticated client transports supply the actor, never the answer payload. */
export const clientHandlers = (
  session: AuthenticatedSession,
  requests: SecretRequests["Service"],
) =>
  SecretRequestClientRpcGroup.of({
    [WS_METHODS.secretsAnswerRequest]: (input) => requests.answer(input, session.sessionId),
  });
