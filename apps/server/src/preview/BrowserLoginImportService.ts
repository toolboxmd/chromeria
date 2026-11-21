import type {
  OrchestratorMcpFailure,
  PreviewAutomationUnavailableError,
  PreviewLoginExportInput,
  PreviewLoginExportResult,
  PreviewLoginImportInput,
  PreviewLoginImportResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type * as McpInvocationContext from "../mcp/McpInvocationContext.ts";

// The tag lives apart from its layer: the MCP tools need the tag, and the layer needs the tools.
/**
 * Copies one site's session between linked environments' server browsers on demand:
 * the destination pulls one bounded snapshot from the peer that shares its logins.
 */
export class BrowserLoginImport extends Context.Service<
  BrowserLoginImport,
  {
    readonly importLogin: (
      scope: McpInvocationContext.McpInvocationScope,
      input: PreviewLoginImportInput,
    ) => Effect.Effect<
      PreviewLoginImportResult,
      OrchestratorMcpFailure | PreviewAutomationUnavailableError
    >;
    readonly exportLogin: (
      scope: McpInvocationContext.McpInvocationScope,
      input: PreviewLoginExportInput,
    ) => Effect.Effect<PreviewLoginExportResult, OrchestratorMcpFailure>;
  }
>()("t3/preview/BrowserLoginImportService/BrowserLoginImport") {}
