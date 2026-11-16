import {
  OrchestratorMcpFailure,
  PreviewAutomationError,
  PreviewLoginExportInput,
  PreviewLoginExportResult,
  PreviewLoginImportInput,
  PreviewLoginImportResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import { McpSchema, Tool, Toolkit } from "effect/ai";

import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as BrowserLoginImport from "../../../preview/BrowserLoginImportService.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ThreadManagementService.ThreadManagementService,
  BrowserLoginImport.BrowserLoginImport,
];

/** Offered only to a linked environment's own requests; agents here never see it. */
const onlyLinkedPeers = (): boolean => {
  const fiber = Fiber.getCurrent();
  const scope =
    fiber && Context.getOrUndefined(fiber.context, McpInvocationContext.McpInvocationContext);
  return scope?.client?.linked === true;
};

const PreviewImportLoginTool = Tool.make("preview_import_login", {
  description:
    "Copy the user's existing session for one site from the linked environment that shares its browser logins into a browser profile here. Use it only after a page you opened shows you are logged out, and at most once per task. In `hosts`, list only hosts that site itself uses, such as admin.shopify.com; never add identity-provider hosts such as accounts.google.com, and Google sites cannot be copied. Then call preview_open with the returned profileId. If the page is still logged out, sign in with preview_fill_credential. Returns counts only; you never see cookie names or values.",
  parameters: PreviewLoginImportInput,
  success: PreviewLoginImportResult,
  failure: Schema.Union([PreviewAutomationError, OrchestratorMcpFailure]),
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Import a browser login")
  .annotate(Tool.OpenWorld, true)
  .annotate(Tool.Destructive, true)
  .annotate(McpSchema.EnabledWhen, McpInvocationContext.hiddenFromOutsideClients);

export const BrowserLoginExportTool = Tool.make("browser_login_export", {
  description:
    "Peer-only: one snapshot of a browser profile's cookies for the listed hosts, for preview_import_login in a linked environment.",
  parameters: PreviewLoginExportInput,
  success: PreviewLoginExportResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Export a browser login")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(McpSchema.EnabledWhen, onlyLinkedPeers);

export const LoginImportToolkit = Toolkit.make(PreviewImportLoginTool);
export const LoginExportToolkit = Toolkit.make(BrowserLoginExportTool);
