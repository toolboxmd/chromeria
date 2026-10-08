# Fork feature map

Only the carried features below and the V2 data foundation belong to this integration stack.
Each JSON entry assigns actual carried edits one primary owner. Shared paths are
watched by other carried features; deferred features are excluded. Keyword matches
are evidence for review, not proof that a feature has been adopted upstream.

## direction

```json
{
  "id": "direction",
  "purpose": "Keep the fork mission and completion criteria explicit.",
  "issues": ["https://github.com/toolboxmd/t3code/issues/5"],
  "prs": ["https://github.com/toolboxmd/t3code/pull/5"],
  "newFiles": ["MISSION.md", "OBJECTIVE.md", "VISION.md"],
  "upstreamFiles": [],
  "sharedFiles": [],
  "keywords": ["project direction", "VISION.md", "MISSION.md", "OBJECTIVE.md"]
}
```

## glossary

```json
{
  "id": "glossary",
  "purpose": "Keep Chromeria and its product family terminology consistent.",
  "issues": ["https://github.com/toolboxmd/t3code/issues/11"],
  "prs": ["https://github.com/toolboxmd/t3code/pull/11"],
  "newFiles": ["GLOSSARY.md"],
  "upstreamFiles": [],
  "sharedFiles": [],
  "keywords": ["Chromeria", "Luxin", "Drafter", "Prism"]
}
```

## branding

```json
{
  "id": "branding",
  "purpose": "Give desktop and web the Chromeria name, icons and independent desktop identity.",
  "issues": [
    "https://github.com/toolboxmd/t3code/issues/12",
    "https://github.com/toolboxmd/t3code/issues/13",
    "https://github.com/toolboxmd/t3code/issues/14"
  ],
  "prs": [
    "https://github.com/toolboxmd/t3code/pull/12",
    "https://github.com/toolboxmd/t3code/pull/13",
    "https://github.com/toolboxmd/t3code/pull/14"
  ],
  "newFiles": [
    "apps/web/public/chromeria-mark.png",
    "assets/chromeria/chromeria-icon-1024.png",
    "assets/chromeria/chromeria-web-apple-touch-180.png",
    "assets/chromeria/chromeria-web-favicon-16x16.png",
    "assets/chromeria/chromeria-web-favicon-32x32.png",
    "assets/chromeria/chromeria-web-favicon.ico",
    "assets/chromeria/chromeria-windows.ico"
  ],
  "upstreamFiles": [
    "apps/desktop/package.json",
    "apps/desktop/src/app/DesktopAppIdentity.test.ts",
    "apps/desktop/src/app/DesktopClerk.test.ts",
    "apps/desktop/src/app/DesktopPreReadyFileSystem.test.ts",
    "apps/desktop/src/app/DesktopEnvironment.ts",
    "apps/desktop/src/app/DesktopPreReadyPlatform.test.ts",
    "apps/desktop/src/app/DesktopUserData.test.ts",
    "apps/desktop/src/app/DesktopUserData.ts",
    "apps/web/index.html",
    "apps/web/src/bootstrap.test.ts",
    "apps/web/src/branding.test.ts",
    "apps/web/src/branding.ts",
    "apps/web/src/bundledDev.test.ts",
    "apps/web/src/components/T3Wordmark.tsx",
    "apps/web/src/components/chat/MessagesTimeline.test.tsx",
    "apps/web/src/components/chat/MessagesTimeline.tsx",
    "apps/web/src/components/onboarding/WelcomeWizard.tsx",
    "apps/web/src/components/settings/IntegrationsSettings.tsx",
    "apps/web/src/components/settings/ThemePreviewCircles.tsx",
    "apps/web/src/components/sidebar/SidebarChrome.tsx",
    "apps/web/src/lib/bootError.ts",
    "scripts/build-desktop-artifact.test.ts",
    "scripts/build-desktop-artifact.ts",
    "scripts/lib/brand-assets.test.ts",
    "scripts/lib/brand-assets.ts",
    "t3.json"
  ],
  "sharedFiles": [],
  "keywords": [
    "branding",
    "productName",
    "appId",
    "wordmark",
    "favicon",
    "Chromeria",
    "auto-update"
  ]
}
```

## fork-maintenance

```json
{
  "id": "fork-maintenance",
  "purpose": "Keep the fork stack small, checked and rebasable on GitHub-hosted CI.",
  "issues": [
    "https://github.com/toolboxmd/t3code/issues/6",
    "https://github.com/toolboxmd/t3code/issues/20"
  ],
  "prs": [
    "https://github.com/toolboxmd/t3code/pull/9",
    "https://github.com/toolboxmd/t3code/pull/24"
  ],
  "newFiles": [
    ".github/workflows/fork.yml",
    "docs/fork-features.md",
    "docs/fork.md",
    "scripts/fork-check.sh",
    "scripts/fork-features.mjs",
    "scripts/fork-maintenance.test.ts",
    "scripts/fork-rebase.sh",
    "scripts/fork-upstream-edits.txt"
  ],
  "upstreamFiles": [
    ".github/workflows/ci.yml",
    ".github/workflows/mobile-fingerprint-check.yml",
    "AGENTS.md",
    "apps/server/src/auth/PairingGrantStore.test.ts",
    "apps/server/src/orchestration-v2/V1ImportBoundary.test.ts",
    "apps/server/src/persistence/AuthPairingLinks.ts",
    "apps/server/src/vcs/GitVcsDriverCore.test.ts",
    "knip.jsonc"
  ],
  "sharedFiles": [],
  "keywords": ["fork", "rebase", "upstream", "blacksmith", "ELECTRON_RUN_AS_NODE", "TMPDIR"]
}
```

## query-interrupt-retry

```json
{
  "id": "query-interrupt-retry",
  "purpose": "Read an environment query again when the server answers it with an interrupt it did not choose, instead of showing the interrupt as an error.",
  "issues": ["https://github.com/toolboxmd/t3code/issues/52"],
  "prs": ["https://github.com/toolboxmd/t3code/pull/54"],
  "newFiles": [],
  "upstreamFiles": [
    "packages/client-runtime/src/state/runtime.test.ts",
    "packages/client-runtime/src/state/runtime.ts"
  ],
  "sharedFiles": [],
  "keywords": ["createEnvironmentQueryAtomFamily", "retryInterruptedRead", "hasInterruptsOnly"]
}
```

## partial-clone-remotes

```json
{
  "id": "partial-clone-remotes",
  "purpose": "Parse `git remote -v` lines that end with a partial-clone filter such as `[blob:none]`, so partial clones keep repository identity and remote lookup.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/135"],
  "prs": ["https://github.com/toolboxmd/chromeria/compare/main...fix/partial-clone-identity"],
  "newFiles": [],
  "upstreamFiles": [
    "apps/server/src/project/RepositoryIdentityResolver.test.ts",
    "apps/server/src/project/RepositoryIdentityResolver.ts",
    "apps/server/src/vcs/GitVcsDriver.test.ts",
    "apps/server/src/vcs/GitVcsDriver.ts",
    "apps/server/src/vcs/GitVcsDriverCore.ts"
  ],
  "sharedFiles": ["apps/server/src/vcs/GitVcsDriverCore.test.ts"],
  "keywords": ["partial clone", "blob:none", "partialclonefilter", "git remote -v"]
}
```

## provider-auto-update

```json
{
  "id": "provider-auto-update",
  "purpose": "Let each environment opt in to installing provider CLI updates without a click.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/159"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/160"],
  "newFiles": [
    "apps/server/src/provider/forkProviderMaintenance.ts",
    "apps/server/src/provider/forkProviderStartAdmission.ts",
    "apps/server/src/provider/providerAdmissionGate.test.ts",
    "apps/server/src/provider/providerAdmissionGate.ts",
    "apps/server/src/provider/providerAutoUpdate.test.ts",
    "apps/server/src/provider/providerAutoUpdate.ts",
    "apps/server/src/provider/providerAutoUpdateState.ts",
    "apps/web/src/components/settings/AutoUpdateProvidersSetting.tsx"
  ],
  "upstreamFiles": [
    "apps/server/src/orchestration-v2/ProviderSessionManager.test.ts",
    "apps/server/src/orchestration-v2/ProviderSessionManager.ts",
    "apps/server/src/provider/providerMaintenanceRunner.test.ts",
    "apps/server/src/provider/providerMaintenanceRunner.ts",
    "apps/server/src/server.ts",
    "apps/server/src/ws.ts",
    "apps/web/src/components/settings/SettingsPanels.tsx",
    "apps/web/src/components/settings/settingsSearch.ts",
    "docs/user/updating.md"
  ],
  "sharedFiles": ["packages/contracts/src/settings.ts", "apps/server/src/serverSettings.test.ts"],
  "keywords": ["autoUpdateProviders", "auto-update providers", "automatic provider updates"]
}
```

## data-foundation

```json
{
  "id": "data-foundation",
  "purpose": "Isolate the Chromeria V2 database, preserve deferred settings data and provide shell-only backfills and fork-origin PR lookup.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/167"],
  "prs": ["https://github.com/toolboxmd/chromeria/compare/fork/v2...port/167-foundation"],
  "newFiles": [
    "apps/server/src/persistence/forkV1Backfills.test.ts",
    "apps/server/src/persistence/forkV1Backfills.ts",
    "apps/server/src/persistence/forkV1Snapshot.testFixtures.ts",
    "apps/server/src/pullRequest/forkRepositoryIdentity.ts",
    "packages/contracts/src/forkSettings.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/persistence/initializeV2Database.test.ts",
    "apps/server/src/cli/config.test.ts",
    "apps/server/src/config.ts",
    "apps/server/src/orchestration-v2/ThreadPullRequestService.test.ts",
    "apps/server/src/orchestration-v2/ThreadPullRequestService.ts",
    "apps/server/src/pullRequest/PullRequestService.test.ts",
    "apps/server/src/pullRequest/PullRequestService.ts",
    "apps/server/src/serverRuntimeStartup.autoPull.test.ts",
    "apps/server/src/serverRuntimeStartup.ts",
    "apps/server/src/serverSettings.test.ts",
    "packages/contracts/src/settings.ts"
  ],
  "sharedFiles": [],
  "keywords": [
    "chromeria-v2.sqlite",
    "forkV1Backfills",
    "reconcileShells",
    "repositoryIdentity",
    "identity.origin",
    "prismRoles",
    "wightModes"
  ]
}
```

## issues-browse

```json
{
  "id": "issues-browse",
  "purpose": "List GitHub Issues of all project repositories beside PRs, with filters, parent tree and a side panel.",
  "issues": [
    "https://github.com/toolboxmd/t3code/issues/27",
    "https://github.com/toolboxmd/t3code/issues/42",
    "https://github.com/toolboxmd/chromeria/issues/171"
  ],
  "prs": [
    "https://github.com/toolboxmd/t3code/pull/32",
    "https://github.com/toolboxmd/chromeria/compare/fork/v2...feat/171-issues-v2"
  ],
  "newFiles": [
    "apps/server/src/issues/IssueService.live.test.ts",
    "apps/server/src/issues/IssueService.test.ts",
    "apps/server/src/issues/IssueService.ts",
    "apps/server/src/issues/gitHubIssues.test.ts",
    "apps/server/src/issues/gitHubIssues.ts",
    "apps/server/src/issues/issueRpcAuthorization.test.ts",
    "apps/server/src/issues/issueRpcHandlers.ts",
    "apps/web/src/components/issues/IssueDetailPanel.tsx",
    "apps/web/src/components/issues/IssueFiltersMenu.tsx",
    "apps/web/src/components/issues/IssuesView.tsx",
    "apps/web/src/components/issues/ListModeToggle.tsx",
    "apps/web/src/components/issues/issueList.logic.test.ts",
    "apps/web/src/components/issues/issueList.logic.ts",
    "apps/web/src/components/issues/issuePaletteItems.tsx",
    "apps/web/src/components/issues/issuePaletteStore.ts",
    "apps/web/src/components/issues/issuePresentation.tsx",
    "apps/web/src/components/pullRequest/pullRequestFilterSearch.logic.test.ts",
    "apps/web/src/components/pullRequest/pullRequestFilterSearch.logic.ts",
    "apps/web/src/state/issues.ts",
    "packages/client-runtime/src/state/issueCommandPermissions.test.ts",
    "packages/contracts/src/issues.test.ts",
    "packages/contracts/src/issues.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/auth/RpcAuthorization.ts",
    "apps/server/src/observability/RpcInstrumentation.ts",
    "apps/web/src/components/CommandPalette.tsx",
    "apps/web/src/components/pullRequest/PullRequestListFilters.tsx",
    "apps/web/src/routes/_chat.pull-requests.tsx",
    "apps/web/src/state/pullRequests.ts",
    "docs/user/source-control.md",
    "packages/contracts/src/clientRpcPermissions.ts",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/rpc.ts"
  ],
  "sharedFiles": [
    "apps/server/src/server.ts",
    "apps/server/src/ws.ts",
    "apps/web/src/components/sidebar/SidebarChrome.tsx"
  ],
  "keywords": [
    "issues",
    "sub-issue",
    "subIssues",
    "is:issue",
    "WsRpcGroup",
    "RPC_REQUIRED_SCOPES",
    "pull-requests route",
    "command palette",
    "GitHubApi",
    "readGraphQlPages",
    "source-control:write"
  ]
}
```

## issues-links

```json
{
  "id": "issues-links",
  "purpose": "Link GitHub Issues to threads, show them beside PRs and start a linked thread from an Issue.",
  "issues": [
    "https://github.com/toolboxmd/t3code/issues/28",
    "https://github.com/toolboxmd/chromeria/issues/171"
  ],
  "prs": [
    "https://github.com/toolboxmd/t3code/pull/34",
    "https://github.com/toolboxmd/chromeria/compare/fork/v2...feat/171-issues-v2"
  ],
  "newFiles": [
    "apps/server/src/issueLinks/IssueLinks.carryover.test.ts",
    "apps/server/src/issueLinks/IssueLinks.test.ts",
    "apps/server/src/issueLinks/IssueLinks.testFixtures.ts",
    "apps/server/src/issueLinks/IssueLinks.ts",
    "apps/server/src/issueLinks/closingReferences.test.ts",
    "apps/server/src/issueLinks/closingReferences.ts",
    "apps/server/src/issueLinks/rpcHandlers.ts",
    "apps/server/src/issueLinks/rpcScopes.ts",
    "apps/server/src/issueLinks/threadIssueLinks.ts",
    "apps/server/src/mcp/toolkits/issues/handlers.ts",
    "apps/server/src/mcp/toolkits/issues/tools.ts",
    "apps/web/src/components/issues/ThreadIssueLinks.tsx",
    "apps/web/src/components/issues/ThreadIssuePanel.tsx",
    "apps/web/src/components/issues/ThreadLinksPanel.tsx",
    "apps/web/src/components/issues/issueLinks.logic.test.ts",
    "apps/web/src/components/issues/issueLinks.logic.ts",
    "apps/web/src/components/issues/useStartThreadFromIssue.ts",
    "apps/web/src/components/issues/useThreadIssueLinks.ts",
    "apps/web/src/components/threadDescendants.logic.test.ts",
    "apps/web/src/components/threadDescendants.logic.ts",
    "apps/web/src/state/issueLinks.ts",
    "apps/web/src/state/threadDescendants.ts",
    "packages/contracts/src/issueLinks.test.ts",
    "packages/contracts/src/issueLinks.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/environment/ServerEnvironment.ts",
    "apps/server/src/mcp/McpHttpServer.ts",
    "apps/server/src/mcp/toolkits/worktree/registration.test.ts",
    "apps/web/src/components/ChatView.tsx",
    "apps/web/src/components/RightPanelTabs.keyboard.test.tsx",
    "apps/web/src/components/RightPanelTabs.tsx",
    "apps/web/src/components/pullRequest/ThreadPullRequestsPanel.tsx",
    "apps/web/src/reopenClosedView.test.ts",
    "apps/web/src/reopenClosedView.ts",
    "apps/web/src/rightPanelStore.test.ts",
    "apps/web/src/rightPanelStore.ts",
    "packages/client-runtime/src/rpc/client.ts",
    "packages/contracts/src/environment.ts"
  ],
  "sharedFiles": [
    "apps/server/src/auth/RpcAuthorization.ts",
    "apps/server/src/observability/RpcInstrumentation.ts",
    "apps/server/src/server.ts",
    "apps/server/src/ws.ts",
    "apps/web/src/components/CommandPalette.tsx",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/rpc.ts"
  ],
  "keywords": [
    "issue link",
    "fork_thread_issue_links",
    "link_issue",
    "closingIssuesReferences",
    "WsRpcGroup",
    "RPC_REQUIRED_SCOPES",
    "ThreadPullRequestsPanel",
    "pullRequestsAvailable",
    "capabilities",
    "Linked pull requests",
    "parentThreadId",
    "relationshipToParent",
    "orchestration_v2_projection_threads",
    "McpToolAccess"
  ]
}
```

## issues-status

```json
{
  "id": "issues-status",
  "purpose": "Compute each Issue's status from GitHub, links and thread activity; group and filter the Issues view by it and wire linked threads and Start thread.",
  "issues": [
    "https://github.com/toolboxmd/t3code/issues/29",
    "https://github.com/toolboxmd/chromeria/issues/171"
  ],
  "prs": [
    "https://github.com/toolboxmd/t3code/pull/36",
    "https://github.com/toolboxmd/chromeria/compare/fork/v2...feat/171-issues-v2"
  ],
  "newFiles": [
    "apps/server/src/issues/reviewMark.test.ts",
    "apps/web/src/components/issues/issuePaletteThreads.logic.test.ts",
    "apps/web/src/components/issues/issuePaletteThreads.logic.ts",
    "apps/web/src/components/issues/issuePaletteThreads.ts",
    "apps/web/src/components/issues/issueStatus.logic.test.ts",
    "apps/web/src/components/issues/issueStatus.logic.ts",
    "apps/web/src/components/issues/useIssueRowThreads.ts",
    "packages/contracts/src/issueStatus.test.ts",
    "packages/contracts/src/issueStatus.ts"
  ],
  "upstreamFiles": [],
  "sharedFiles": [
    "apps/server/src/environment/ServerEnvironment.ts",
    "apps/web/src/components/CommandPalette.tsx",
    "apps/web/src/components/sidebar/SidebarChrome.tsx",
    "apps/web/src/routes/_chat.pull-requests.tsx",
    "docs/user/source-control.md",
    "packages/contracts/src/environment.ts"
  ],
  "keywords": [
    "review/independent",
    "closedByPullRequestsReferences",
    "issueDependenciesSummary",
    "blockedBy",
    "statuses",
    "pull-requests route",
    "pendingBackgroundTasks"
  ]
}
```

## issues-open-links

```json
{
  "id": "issues-open-links",
  "purpose": "Open GitHub /issues/N links and an Issue's pull requests in the app: the PR panel for pull requests, the Issues panel for Issues, the browser otherwise.",
  "issues": [
    "https://github.com/toolboxmd/t3code/issues/40",
    "https://github.com/toolboxmd/chromeria/issues/171"
  ],
  "prs": [
    "https://github.com/toolboxmd/t3code/pull/41",
    "https://github.com/toolboxmd/chromeria/compare/fork/v2...feat/171-issues-v2"
  ],
  "newFiles": [
    "apps/web/src/components/issues/issueLinkOpening.logic.test.ts",
    "apps/web/src/components/issues/issueLinkOpening.logic.ts",
    "apps/web/src/components/issues/useOpenIssueOrPullRequestLink.ts"
  ],
  "upstreamFiles": [
    "apps/web/src/components/ChatMarkdown.tsx",
    "apps/web/src/lib/openPullRequestLink.ts"
  ],
  "sharedFiles": [
    "apps/web/src/reopenClosedView.ts",
    "apps/web/src/rightPanelStore.ts",
    "docs/user/source-control.md"
  ],
  "keywords": [
    "pullRequestCandidateUrlFromReferenceAutolink",
    "useOpenChangeRequestLink",
    "PullRequestLinkPreview",
    "MarkdownAnchor",
    "isPullRequestNotFound"
  ]
}
```

## Thread people

`SubagentProjection.ts` is temporarily owned here; primary ownership transfers to
child-threads when #168 lands, and thread-people will watch it as a shared path.

```json
{
  "id": "thread-people",
  "purpose": "Label devices, filter threads by person and share threads, as views without access control.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/121",
    "https://github.com/toolboxmd/chromeria/issues/170"
  ],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/131",
    "https://github.com/toolboxmd/chromeria/pull/180"
  ],
  "newFiles": [
    "apps/server/src/mcp/toolkits/thread/handlers.test.ts",
    "apps/server/src/orchestration-v2/ThreadPeople.test.ts",
    "apps/server/src/orchestration-v2/ThreadPeople.ts",
    "apps/server/src/persistence/forkThreadPeopleBackfill.test.ts",
    "apps/server/src/persistence/forkThreadPeopleBackfill.ts",
    "apps/server/src/persistence/forkThreadPeopleSchema.ts",
    "apps/web/src/components/people/ClientPersonSelect.tsx",
    "apps/web/src/components/people/PersonPicker.tsx",
    "apps/web/src/components/people/SharedThreadLabel.tsx",
    "apps/web/src/components/people/ThreadSharingControl.tsx",
    "apps/web/src/components/people/personView.test.ts",
    "apps/web/src/components/people/personView.ts",
    "apps/web/src/components/people/threadSharing.ts",
    "apps/web/src/components/people/usePersonView.ts",
    "docs/user/people.md",
    "packages/contracts/src/people.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/auth/EnvironmentAuth.test.ts",
    "apps/server/src/auth/EnvironmentAuth.ts",
    "apps/server/src/auth/SessionStore.test.ts",
    "apps/server/src/auth/SessionStore.ts",
    "apps/server/src/auth/http.ts",
    "apps/server/src/mcp/OrchestratorMcpService.ts",
    "apps/server/src/mcp/OrchestratorMcpToolkit.integration.test.ts",
    "apps/server/src/mcp/toolkits/project/handlers.test.ts",
    "apps/server/src/mcp/toolkits/project/handlers.ts",
    "apps/server/src/mcp/toolkits/thread/handlers.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "apps/server/src/orchestration-v2/ProjectionStore.ts",
    "apps/server/src/orchestration-v2/ThreadLaunchService.ts",
    "apps/server/src/orchestration-v2/testkit/OrchestratorScenario.ts",
    "apps/server/src/persistence/AuthSessions.ts",
    "apps/server/src/persistence/Sqlite.ts",
    "apps/web/src/components/Sidebar.tsx",
    "apps/web/src/components/chat/ChatHeader.tsx",
    "apps/web/src/components/settings/ConnectionsSettings.tsx",
    "apps/web/src/environments/primary/auth.ts",
    "apps/web/src/environments/primary/index.ts",
    "apps/web/test/environmentHttpTest.ts",
    "docs/README.md",
    "packages/contracts/src/auth.ts",
    "packages/contracts/src/environmentHttp.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "apps/server/src/orchestration-v2/SubagentProjection.ts"
  ],
  "sharedFiles": [
    "packages/contracts/src/index.ts",
    "apps/server/src/ws.ts",
    "apps/server/src/orchestration-v2/V1ImportBoundary.test.ts",
    "apps/server/src/persistence/forkV1Backfills.ts"
  ],
  "keywords": ["thread ownership", "coOwners", "person picker", "thread.share"]
}
```

## child-threads

```json
{
  "id": "child-threads",
  "purpose": "Create native children in any project, handle descendant requests and preserve durable subtree Stop.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/168"],
  "prs": ["https://github.com/toolboxmd/chromeria/compare/fork/v2...feat/168-child-threads-v2"],
  "newFiles": [
    "apps/server/src/childThreads/ForkCommitPlan.ts",
    "apps/server/src/childThreads/crossProject.test.ts",
    "apps/server/src/childThreads/lineageBackfill.test.ts",
    "apps/server/src/childThreads/lineageBackfill.ts",
    "apps/server/src/childThreads/pendingRequests.test.ts",
    "apps/server/src/childThreads/pendingRequests.ts",
    "apps/server/src/childThreads/providerStop.test.ts",
    "apps/server/src/childThreads/requestWake.test.ts",
    "apps/server/src/childThreads/requestWake.ts",
    "apps/server/src/childThreads/retirement.test.ts",
    "apps/server/src/childThreads/retirement.ts",
    "apps/server/src/childThreads/stopDescendants.ts",
    "apps/server/src/childThreads/workspaceAccess.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/mcp/OrchestratorMcpService.activity.test.ts",
    "apps/server/src/mcp/OrchestratorMcpService.test.ts",
    "apps/server/src/mcp/OrchestratorMcpService.ts",
    "apps/server/src/mcp/OrchestratorMcpToolkit.integration.test.ts",
    "apps/server/src/mcp/threadAccess.ts",
    "apps/server/src/mcp/toolkits/core.test.ts",
    "apps/server/src/mcp/toolkits/thread/handlers.ts",
    "apps/server/src/mcp/toolkits/thread/tools.ts",
    "apps/server/src/orchestration-v2/EffectOutbox.ts",
    "apps/server/src/orchestration-v2/EffectWorker.ts",
    "apps/server/src/orchestration-v2/EventSink.ts",
    "apps/server/src/orchestration-v2/FoundationPersistence.test.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "apps/server/src/orchestration-v2/ThreadLaunchService.test.ts",
    "apps/server/src/orchestration-v2/ThreadLaunchService.ts",
    "apps/server/src/orchestration-v2/ThreadManagementService.ts",
    "apps/server/src/orchestration-v2/ThreadStop.test.ts",
    "apps/server/src/orchestration-v2/testkit/ProviderReplayHarness.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "packages/contracts/src/orchestratorMcp.ts"
  ],
  "sharedFiles": ["apps/server/src/persistence/forkV1Backfills.ts", "apps/server/src/mcp/toolkits/worktree/registration.test.ts"],
  "keywords": [
    "delegate_task",
    "delegated_task.request",
    "forkRetirement",
    "forkResumedRetirements",
    "subagent_result",
    "lineageBackfill"
  ]
}
```
