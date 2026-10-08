import { describe, expect, it } from "vite-plus/test";

import { AuthSourceControlWriteScope } from "./auth.ts";
import { clientRpcRequiredScopes } from "./clientRpcPermissions.ts";
import { ISSUE_WS_METHODS } from "./issues.ts";

describe("Issue RPC permissions", () => {
  it("guard commenting and closing on the client with the source-control write grant", () => {
    for (const method of [ISSUE_WS_METHODS.issuesComment, ISSUE_WS_METHODS.issuesSetState]) {
      expect(clientRpcRequiredScopes(method, undefined)).toEqual([AuthSourceControlWriteScope]);
    }
  });
  it("leave reads to the server's own read scope", () => {
    for (const method of [
      ISSUE_WS_METHODS.issuesList,
      ISSUE_WS_METHODS.issuesDetail,
      ISSUE_WS_METHODS.issuesStates,
    ]) {
      expect(clientRpcRequiredScopes(method, undefined)).toEqual([]);
    }
  });
});
