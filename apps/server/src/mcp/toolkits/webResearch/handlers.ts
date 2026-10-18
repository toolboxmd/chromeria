import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as WebResearch from "../../../webResearch/WebResearch.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { WebResearchToolkit } from "./tools.ts";

export const layer = McpToolAccess.toLayer(WebResearchToolkit, {
  web_read: McpToolAccess.readsAsCaller((input) =>
    WebResearch.WebResearch.use((research) => research.read(input)).pipe(
      Effect.mapError(
        (error) =>
          new OrchestratorMcpFailure({
            code:
              error._tag === "WebResearchInputError" ? "invalid_request" : "orchestration_error",
            message: error.message,
          }),
      ),
    ),
  ),
});
