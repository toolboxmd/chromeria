import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as ResearchHost from "../../../webResearch/ResearchHost.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { WebResearchToolkit } from "./tools.ts";

export const layer = McpToolAccess.toLayer(WebResearchToolkit, {
  web_read: McpToolAccess.readsAsCaller(
    (input) =>
      ResearchHost.ResearchHost.use((research) => research.read(input)).pipe(
        Effect.mapError((error) =>
          error._tag === "OrchestratorMcpFailure"
            ? error
            : new OrchestratorMcpFailure({
                code:
                  error._tag === "ResearchHostUnavailable"
                    ? "research_host_unavailable"
                    : error._tag === "WebResearchInputError" ||
                        error._tag === "ResearchCacheOwnerError"
                      ? "invalid_request"
                      : "orchestration_error",
                message: error.message,
              }),
        ),
      ),
    { linkedCallers: "allowed" },
  ),
});
