import type { OrchestrationV2Command } from "@t3tools/contracts";

/**
 * Only the server records who is the Promachos and who answered for the user
 * (toolboxmd/chromeria#532), so a client command cannot claim either.
 */
export const withoutPromachosClaims = (command: OrchestrationV2Command): OrchestrationV2Command => {
  switch (command.type) {
    case "thread.create": {
      const { prismRole: _prismRole, ...rest } = command;
      return rest;
    }
    case "runtime-request.respond": {
      const { answeredBy: _answeredBy, ...rest } = command;
      return rest;
    }
    default:
      return command;
  }
};
