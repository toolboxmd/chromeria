import * as Effect from "effect/Effect";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as BrowserLoginImport from "../../../preview/BrowserLoginImportService.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { LoginExportToolkit, LoginImportToolkit } from "./tools.ts";

export const layerImport = McpToolAccess.toLayer(LoginImportToolkit, {
  preview_import_login: McpToolAccess.actsAsCaller(
    (input) =>
      Effect.all([
        BrowserLoginImport.BrowserLoginImport,
        McpInvocationContext.McpInvocationContext,
      ]).pipe(Effect.flatMap(([service, scope]) => service.importLogin(scope, input))),
    "external-unknown",
  ),
});

// The service refuses unless an accepted direct peer action carries a current export grant.
export const layerExport = McpToolAccess.toLayer(LoginExportToolkit, {
  browser_login_export: McpToolAccess.readsAsCaller(
    (input) =>
      Effect.all([
        BrowserLoginImport.BrowserLoginImport,
        McpInvocationContext.McpInvocationContext,
      ]).pipe(Effect.flatMap(([service, scope]) => service.exportLogin(scope, input))),
    { linkedCallers: "allowed" },
  ),
});
