# Fork feature map

Only the carried features below and the V2 data foundation belong to this integration stack.
Each JSON entry assigns actual carried edits one primary owner. Shared paths are
watched by other carried features; deferred features are excluded. Keyword matches
are evidence for review, not proof that a feature has been adopted upstream.

## spectrum

```json
{
  "id": "spectrum",
  "purpose": "Run council and free Spectrum conversations with durable server-authored transcripts and exact-run barriers.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/176"],
  "prs": ["https://github.com/toolboxmd/chromeria/compare/fork/v2...feat/176-spectrum-v2"],
  "newFiles": [
    "apps/mobile/src/features/settings/spectrumReportAction.tsx",
    "apps/server/src/fork/ForkCommandInterceptor.ts",
    "apps/server/src/spectrum/Controller.integration.test.ts",
    "apps/server/src/spectrum/Controller.ts",
    "apps/server/src/spectrum/Launch.integration.test.ts",
    "apps/server/src/spectrum/LaunchService.ts",
    "apps/server/src/spectrum/ReportService.integration.test.ts",
    "apps/server/src/spectrum/ReportService.ts",
    "apps/server/src/spectrum/RoundService.ts",
    "apps/server/src/spectrum/SchedulerAdapter.ts",
    "apps/server/src/spectrum/TranscriptService.test.ts",
    "apps/server/src/spectrum/TranscriptService.ts",
    "apps/server/src/spectrum/barrier.test.ts",
    "apps/server/src/spectrum/barrier.ts",
    "apps/server/src/spectrum/cancellationPlan.ts",
    "apps/server/src/spectrum/commandPlan.ts",
    "apps/server/src/spectrum/controllerTestkit.ts",
    "apps/server/src/spectrum/humanReportRpc.test.ts",
    "apps/server/src/spectrum/humanReportRpc.ts",
    "apps/server/src/spectrum/humanStopRpc.ts",
    "apps/server/src/spectrum/humanStopRpc.integration.test.ts",
    "apps/server/src/spectrum/launchAdmission.ts",
    "apps/server/src/spectrum/lifecycle.ts",
    "apps/server/src/spectrum/mcpInterrupt.ts",
    "apps/server/src/spectrum/mcpSend.integration.test.ts",
    "apps/server/src/spectrum/mcpScopes.test.ts",
    "apps/server/src/spectrum/mcpSend.ts",
    "apps/server/src/spectrum/mcpToolkit.ts",
    "apps/server/src/spectrum/registrationPlan.ts",
    "apps/server/src/spectrum/resumeScheduler.integration.test.ts",
    "apps/server/src/spectrum/resumeScheduler.ts",
    "apps/server/src/spectrum/reportPolicy.ts",
    "apps/server/src/spectrum/runtimeLayer.ts",
    "apps/server/src/spectrum/runtimeLayer.integration.test.ts",
    "apps/server/src/spectrum/spectrumPlan.test.ts",
    "apps/server/src/spectrum/spectrumPlan.ts",
    "apps/server/src/spectrum/state.ts",
    "apps/server/src/spectrum/store.test.ts",
    "apps/server/src/spectrum/store.ts",
    "apps/server/src/spectrum/testFixtures.ts",
    "apps/server/src/spectrum/transcript.test.ts",
    "apps/server/src/spectrum/transcript.ts",
    "apps/web/src/components/settings/spectrumReportAction.tsx",
    "apps/web/src/spectrumStop.ts",
    "apps/web/src/spectrumStop.test.ts",
    "packages/client-runtime/src/spectrumReport.test.ts",
    "packages/client-runtime/src/spectrumReport.ts",
    "packages/client-runtime/src/spectrumStop.ts",
    "packages/client-runtime/src/spectrumStop.test.ts",
    "packages/contracts/src/spectrum.ts",
    "packages/contracts/src/spectrumRpc.test.ts",
    "packages/contracts/src/spectrumRpc.ts"
  ],
  "upstreamFiles": [
    "apps/mobile/src/features/threads/ThreadRouteScreen.tsx",
    "apps/mobile/src/state/use-thread-selection.ts",
    "packages/client-runtime/src/state/threadCommands.ts",
    "vite.config.ts"
  ],
  "sharedFiles": [
    "apps/server/src/orchestration-v2/ThreadLaunchService.ts",
    "apps/server/src/orchestration-v2/ThreadLaunchService.test.ts",
    "apps/mobile/src/features/settings/SettingsScheduledTasksRouteScreen.tsx",
    "apps/mobile/src/features/settings/scheduledTaskFork.tsx",
    "apps/server/src/childThreads/ForkCommitPlan.ts",
    "apps/server/src/childThreads/retirement.ts",
    "apps/server/src/fork/ForkDispatchPlans.ts",
    "apps/server/src/fork/commitSequence.test.ts",
    "apps/server/src/fork/commitSequence.ts",
    "apps/server/src/mcp/McpHttpServer.ts",
    "apps/server/src/mcp/OrchestratorMcpService.ts",
    "apps/server/src/mcp/toolkits/orchestrator/tools.ts",
    "apps/server/src/observability/RpcInstrumentation.ts",
    "apps/server/src/orchestration-v2/EventSink.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "apps/server/src/orchestration-v2/ProjectionStore.ts",
    "apps/server/src/orchestration-v2/runtimeLayer.ts",
    "apps/server/src/orchestration-v2/testkit/ProviderReplayHarness.ts",
    "apps/server/src/persistence/forkV1Backfills.ts",
    "apps/server/src/prism/PrismService.ts",
    "apps/server/src/ws.ts",
    "docs/user/project-settings.md",
    "apps/web/src/components/settings/scheduledTaskFork.tsx",
    "apps/web/src/components/ChatView.tsx",
    "packages/client-runtime/package.json",
    "packages/client-runtime/src/operations/commands.ts",
    "packages/contracts/src/clientRpcPermissions.ts",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/orchestratorMcp.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "packages/contracts/src/rpc.ts"
  ],
  "keywords": [
    "Spectrum",
    "fork_spectra",
    "spectrum.transcript.append",
    "server-authored transcript append"
  ]
}
```

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
  "purpose": "Give desktop, web and the iOS app the Chromeria name, icons and independent identity.",
  "issues": [
    "https://github.com/toolboxmd/t3code/issues/12",
    "https://github.com/toolboxmd/t3code/issues/13",
    "https://github.com/toolboxmd/t3code/issues/14",
    "https://github.com/toolboxmd/chromeria/issues/205"
  ],
  "prs": [
    "https://github.com/toolboxmd/t3code/pull/12",
    "https://github.com/toolboxmd/t3code/pull/13",
    "https://github.com/toolboxmd/t3code/pull/14",
    "https://github.com/toolboxmd/chromeria/pull/209"
  ],
  "newFiles": [
    "apps/mobile/chromeria.ts",
    "apps/web/public/chromeria-mark.png",
    "assets/chromeria/chromeria-icon-1024.png",
    "assets/chromeria/chromeria-ios-1024.png",
    "assets/chromeria/chromeria-web-apple-touch-180.png",
    "assets/chromeria/chromeria-web-favicon-16x16.png",
    "assets/chromeria/chromeria-web-favicon-32x32.png",
    "assets/chromeria/chromeria-web-favicon.ico",
    "assets/chromeria/chromeria-windows.ico"
  ],
  "upstreamFiles": [
    "apps/desktop/package.json",
    "apps/mobile/app.config.ts",
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
    "auto-update",
    "bundleIdentifier",
    "appleTeamId"
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
    "apps/server/src/provider/acp/AcpSessionRuntime.processTree.test.ts",
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

Child creation paths are owned by child-threads and shared with thread-people.

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
    "apps/server/src/mcp/toolkits/project/handlers.test.ts",
    "apps/server/src/mcp/toolkits/project/handlers.ts",
    "apps/server/src/orchestration-v2/ProjectionStore.ts",
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
    "packages/contracts/src/environmentHttp.ts"
  ],
  "sharedFiles": [
    "packages/contracts/src/index.ts",
    "apps/server/src/ws.ts",
    "apps/server/src/orchestration-v2/V1ImportBoundary.test.ts",
    "apps/server/src/persistence/forkV1Backfills.ts",
    "apps/server/src/mcp/OrchestratorMcpService.ts",
    "apps/server/src/mcp/OrchestratorMcpToolkit.integration.test.ts",
    "apps/server/src/mcp/toolkits/thread/handlers.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "apps/server/src/orchestration-v2/ThreadLaunchService.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "apps/server/src/orchestration-v2/SubagentProjection.ts"
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
  "prs": ["https://github.com/toolboxmd/chromeria/pull/181"],
  "newFiles": [
    "apps/server/src/childThreads/ForkCommitPlan.ts",
    "apps/server/src/childThreads/crossProject.test.ts",
    "apps/server/src/childThreads/delegatedIntake.ts",
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
    "apps/server/src/childThreads/workspaceAccess.ts",
    "apps/server/src/fork/ForkDispatchPlans.ts",
    "apps/server/src/fork/commitSequence.ts",
    "apps/server/src/fork/commitSequence.test.ts"
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
    "apps/server/src/orchestration-v2/ThreadMessageIntake.test.ts",
    "apps/server/src/orchestration-v2/ThreadMessageIntake.ts",
    "apps/server/src/orchestration-v2/ThreadManagementService.ts",
    "apps/server/src/orchestration-v2/ThreadStop.test.ts",
    "apps/server/src/orchestration-v2/testkit/ProviderReplayHarness.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "packages/contracts/src/orchestratorMcp.ts",
    "apps/server/src/orchestration-v2/SubagentProjection.ts"
  ],
  "sharedFiles": [
    "apps/server/src/persistence/forkV1Backfills.ts",
    "apps/server/src/mcp/toolkits/worktree/registration.test.ts",
    "apps/server/src/ws.ts"
  ],
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

## prism-toolkit

```json
{
  "id": "prism-toolkit",
  "purpose": "Route upstream delegation and top-level launches through one Prism service and coordinate persisted same-provider/model recovery with immutable per-run outcomes, indexed admitted continuation sources and current recovery intent. Own Chromeria autoResumeLimitedThreads=true; Wight consumes it and saved false overrides remain respected.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/169"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/183"],
  "newFiles": [
    "apps/server/src/prism/PrismService.test.ts",
    "apps/server/src/prism/PrismService.ts",
    "apps/server/src/prism/Recovery.integration.test.ts",
    "apps/server/src/prism/RecoveryCoordinator.test.ts",
    "apps/server/src/prism/RecoveryCoordinator.ts",
    "apps/server/src/prism/RecoveryHooks.ts",
    "apps/server/src/prism/RecoveryHistory.ts",
    "apps/server/src/prism/RecoveryHistory.integration.test.ts",
    "apps/server/src/prism/continuationAdmission.ts",
    "apps/server/src/prism/continuationProjection.ts",
    "apps/server/src/prism/recoveryOutcomePolicy.ts",
    "apps/server/src/prism/recoveryProjection.ts",
    "apps/server/src/prism/RecoveryReactor.test.ts",
    "apps/server/src/prism/RecoveryReactor.ts",
    "apps/server/src/prism/RecoverySettings.test.ts",
    "apps/server/src/prism/RecoveryStore.test.ts",
    "apps/server/src/prism/RecoveryStore.ts",
    "apps/server/src/prism/recovery.testkit.ts",
    "apps/server/src/prism/recoveryAdmission.test.ts",
    "apps/server/src/prism/recoveryAdmission.ts",
    "apps/server/src/prism/recoveryPolicy.ts",
    "packages/contracts/src/prism.test.ts",
    "packages/contracts/src/prism.ts"
  ],
  "upstreamFiles": [
    "apps/web/src/components/settings/SettingsPanels.restore.test.tsx",
    "apps/server/src/mcp/toolkits/project/tools.ts",
    "apps/server/src/orchestration-v2/runtimeLayer.ts"
  ],
  "sharedFiles": [
    "apps/server/src/mcp/toolkits/project/handlers.test.ts",
    "apps/server/src/mcp/toolkits/project/handlers.ts",
    "packages/contracts/src/index.ts",
    "apps/server/src/mcp/OrchestratorMcpService.activity.test.ts",
    "apps/server/src/mcp/OrchestratorMcpService.test.ts",
    "apps/server/src/mcp/OrchestratorMcpService.ts",
    "apps/server/src/mcp/OrchestratorMcpToolkit.integration.test.ts",
    "apps/server/src/mcp/toolkits/core.test.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "apps/server/src/orchestration-v2/ProjectionStore.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "packages/contracts/src/orchestratorMcp.ts",
    "packages/contracts/src/settings.ts"
  ],
  "keywords": [
    "Prism",
    "delegate_task",
    "t3_thread_launch",
    "prismRole",
    "forkPrismRetryOfRunId",
    "forkPrismContinuationSourceRunId",
    "readRecoveryState",
    "autoResumeLimitedThreads",
    "limitRecovery",
    "forkRetirement"
  ]
}
```

## prism-settings

```json
{
  "id": "prism-settings",
  "purpose": "Keep scoped Prism kits, ordered model lists and worker lanes editable with saved settings replay and honest explicit retry/escalation kit semantics.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/169"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/183"],
  "newFiles": [
    "apps/web/src/components/settings/PrismSettings.logic.test.ts",
    "apps/web/src/components/settings/PrismSettings.logic.ts",
    "apps/web/src/components/settings/PrismSettings.state.test.ts",
    "apps/web/src/components/settings/PrismSettings.state.ts",
    "apps/web/src/components/settings/PrismSettings.tsx",
    "apps/web/src/routes/settings.prism.tsx"
  ],
  "upstreamFiles": [
    "apps/web/src/components/settings/SettingsSidebarNav.tsx",
    "apps/web/src/routeTree.gen.ts"
  ],
  "sharedFiles": [
    "apps/server/src/serverSettings.test.ts",
    "apps/web/src/components/settings/settingsSearch.ts",
    "packages/contracts/src/settings.ts",
    "packages/contracts/src/prism.ts",
    "packages/contracts/src/prism.test.ts"
  ],
  "keywords": ["prismRoles", "PrismSettings", "worker lanes", "prismRole", "Retry", "Escalation"]
}
```

## tool-instructions

```json
{
  "id": "tool-instructions",
  "purpose": "Give every provider runtime consistent visible T3 delegation and Prism routing/recovery guidance.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/169",
    "https://github.com/toolboxmd/chromeria/issues/203"
  ],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/183",
    "https://github.com/toolboxmd/chromeria/pull/204"
  ],
  "newFiles": [
    "apps/server/src/mcp/delegatedTaskContinuation.test.ts",
    "apps/server/src/mcp/toolInstructions.ts",
    "apps/server/src/prism/ReplayInstructions.test.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/mcp/toolkits/orchestrator/tools.ts",
    "apps/server/src/provider/RuntimeInstructions.ts",
    "apps/server/src/provider/T3OrchestrationInstructions.ts",
    "apps/server/src/orchestration-v2/testkit/ReplayTranscriptNdjson.ts"
  ],
  "sharedFiles": [
    "apps/server/src/mcp/toolkits/project/tools.ts",
    "packages/contracts/src/orchestratorMcp.ts"
  ],
  "keywords": [
    "t3_code_tool_use",
    "delegate_task",
    "t3_thread_launch",
    "Prism",
    "prismRole",
    "shell agent"
  ]
}
```

## prism-stream-clock

```json
{
  "id": "prism-stream-clock",
  "purpose": "Measure attempt-owned provider activity in memory and persist one idempotent healthy sample per finished provider turn across restarts, without importing v1 activities.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/172"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/184"],
  "newFiles": [
    "apps/server/src/prism/ProviderEventIngestor.ts",
    "apps/server/src/prism/StreamStatsStore.ts",
    "apps/server/src/prism/StreamStatsStore.test.ts",
    "apps/server/src/prism/streamClock.ts",
    "apps/server/src/prism/streamClock.test.ts",
    "apps/server/src/prism/streamClock.integration.test.ts"
  ],
  "upstreamFiles": ["apps/server/src/orchestration-v2/RunExecutionService.ts"],
  "sharedFiles": ["apps/server/src/orchestration-v2/runtimeLayer.ts"],
  "keywords": [
    "StreamClock",
    "StreamStatsStore",
    "fork_prism_stream_stats",
    "StreamClockAttempt",
    "StreamClockHooks"
  ]
}
```

## prism-stale-turn-detector

```json
{
  "id": "prism-stale-turn-detector",
  "purpose": "Send one advisory parent notice for a silent active child; exclude host suspension, open tools and retired or waiting work. V2 and Prism recovery retain terminal failure delivery.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/172"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/184"],
  "newFiles": [
    "apps/server/src/prism/staleTurnDetector.ts",
    "apps/server/src/prism/staleTurnDetector.test.ts",
    "apps/server/src/prism/staleTurnMonitor.ts",
    "apps/server/src/prism/staleTurnMonitor.integration.test.ts"
  ],
  "upstreamFiles": [],
  "sharedFiles": ["apps/server/src/orchestration-v2/runtimeLayer.ts"],
  "keywords": ["StaleTurnMonitor", "makeStaleTurnDetectorState", "queue_after_active", "silence"]
}
```

## wight-mode

```json
{
  "id": "wight-mode",
  "purpose": "Continue idle threads until their timer or instance quota pauses them, preserving retirement and stored effort.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/173"],
  "prs": ["https://github.com/toolboxmd/chromeria/compare/fork/v2...feat/173-wight-v2"],
  "newFiles": [
    "apps/server/src/wight/AdmissionHooks.ts",
    "apps/server/src/wight/WightService.ts",
    "apps/server/src/wight/admission.integration.test.ts",
    "apps/server/src/wight/admission.ts",
    "apps/server/src/wight/wightMode.test.ts",
    "apps/server/src/wight/wightMode.ts",
    "apps/web/src/components/chat/WightModeControl.tsx",
    "apps/web/src/components/settings/WightLimitSetting.tsx",
    "apps/web/src/wightMode.test.ts",
    "apps/web/src/wightMode.ts",
    "packages/contracts/src/wight.test.ts",
    "packages/contracts/src/wight.ts",
    "packages/shared/src/wightSettings.test.ts"
  ],
  "upstreamFiles": [
    "apps/web/src/components/settings/ProviderInstanceCard.tsx",
    "docs/user/thread-sidebar.md",
    "packages/contracts/src/providerInstance.ts",
    "packages/shared/src/serverSettings.ts"
  ],
  "sharedFiles": [
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "apps/server/src/orchestration-v2/runtimeLayer.ts",
    "apps/server/src/serverSettings.test.ts",
    "apps/web/src/components/chat/ChatHeader.tsx",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "packages/contracts/src/settings.ts"
  ],
  "keywords": ["Wight", "wightModes", "wightLimitPercent", "wightAdmission"]
}
```

## promachos-mode

```json
{
  "id": "promachos-mode",
  "purpose": "Chat with the Promachos in bubbles from his persona home, using Prism for the initial launch.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/175"],
  "prs": ["https://github.com/toolboxmd/chromeria/compare/fork/v2...feat/175-promachos-v2"],
  "newFiles": [
    "apps/server/src/promachos/PromachosHome.test.ts",
    "apps/server/src/promachos/PromachosHome.ts",
    "apps/server/src/promachos/PromachosLaunch.ts",
    "apps/server/src/promachos/PromachosLaunch.tests.ts",
    "apps/server/src/promachos/PromachosRpc.ts",
    "apps/web/src/components/promachos/PromachosChat.tsx",
    "apps/web/src/components/promachos/PromachosModeSwitch.tsx",
    "apps/web/src/components/promachos/PromachosNewHome.tsx",
    "apps/web/src/components/promachos/PromachosSidebar.tsx",
    "apps/web/src/components/promachos/promachosBubbles.test.tsx",
    "apps/web/src/components/promachos/promachosBubbles.ts",
    "apps/web/src/components/promachos/promachosConversations.test.ts",
    "apps/web/src/components/promachos/promachosConversations.ts",
    "apps/web/src/components/promachos/promachosMode.ts",
    "apps/web/src/components/promachos/promachosStart.test.ts",
    "apps/web/src/components/promachos/promachosStart.ts",
    "apps/web/src/components/promachos/promachosTimeline.test.ts",
    "apps/web/src/components/promachos/promachosTimeline.ts",
    "docs/user/promachos-mode.md",
    "packages/contracts/src/promachosHome.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/auth/RpcAuthorization.test.ts",
    "apps/web/src/components/AppSidebarLayout.tsx",
    "apps/web/src/components/chat/ChatComposer.tsx",
    "packages/client-runtime/src/operations/commands.test.ts",
    "packages/client-runtime/src/operations/commands.ts"
  ],
  "sharedFiles": [
    "apps/server/src/observability/RpcInstrumentation.ts",
    "docs/README.md",
    "apps/server/src/orchestration-v2/ThreadLaunchService.test.ts",
    "apps/server/src/ws.ts",
    "apps/web/src/components/ChatView.tsx",
    "apps/web/src/components/chat/MessagesTimeline.tsx",
    "apps/web/src/components/sidebar/SidebarChrome.tsx",
    "packages/contracts/src/clientRpcPermissions.ts",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "packages/contracts/src/rpc.ts"
  ],
  "keywords": ["Promachos", "promachos", "PROMACHOS_BUBBLE_MARKDOWN", "prismRole"]
}
```

## scheduled-tasks

```json
{
  "id": "scheduled-tasks",
  "purpose": "Extend upstream's single scheduler with outcome checks that keep one thread working until a pinned check passes, one-shot and weekly triggers, Prism roles, shell command tasks with failure alerts, the Spectrum report completion fence, and the Chromeria v1 task import.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/174"],
  "prs": ["https://github.com/toolboxmd/chromeria/compare/fork/v2...feat/174-scheduled-tasks-v2"],
  "newFiles": [
    "apps/mobile/src/features/settings/scheduledTaskDraftFork.test.ts",
    "apps/mobile/src/features/settings/scheduledTaskFork.tsx",
    "apps/server/src/scheduledTaskChecks/DispatchPolicy.ts",
    "apps/server/src/scheduledTaskChecks/ScheduledTaskChecks.integration.test.ts",
    "apps/server/src/scheduledTaskChecks/ScheduledTaskChecks.ts",
    "apps/server/src/scheduledTaskChecks/commandRunner.test.ts",
    "apps/server/src/scheduledTaskChecks/commandRunner.ts",
    "apps/server/src/scheduledTaskChecks/engine.test.ts",
    "apps/server/src/scheduledTaskChecks/engine.ts",
    "apps/server/src/scheduledTaskChecks/handoff.integration.test.ts",
    "apps/server/src/scheduledTaskChecks/handoff.test.ts",
    "apps/server/src/scheduledTaskChecks/handoff.ts",
    "apps/server/src/scheduledTaskChecks/schedules.test.ts",
    "apps/server/src/scheduledTaskChecks/schedules.ts",
    "apps/server/src/scheduledTaskChecks/spectra.testkit.ts",
    "apps/server/src/scheduledTaskChecks/state.ts",
    "apps/server/src/scheduledTaskChecks/store.ts",
    "apps/server/src/scheduledTaskChecks/v1Import.actual.integration.test.ts",
    "apps/server/src/scheduledTaskChecks/v1Import.test.ts",
    "apps/server/src/scheduledTaskChecks/v1Import.ts",
    "apps/web/src/components/ScheduledCommandNotifications.test.tsx",
    "apps/web/src/components/ScheduledCommandNotifications.tsx",
    "apps/web/src/components/settings/scheduledTaskFork.logic.test.ts",
    "apps/web/src/components/settings/scheduledTaskFork.tsx",
    "packages/client-runtime/src/scheduledTaskFork.test.ts",
    "packages/client-runtime/src/scheduledTaskFork.ts",
    "packages/contracts/src/scheduledTaskChecks.test.ts",
    "packages/contracts/src/scheduledTaskChecks.ts"
  ],
  "upstreamFiles": [
    "apps/mobile/src/features/settings/SettingsScheduledTasksRouteScreen.tsx",
    "apps/mobile/src/features/settings/scheduledTaskDraft.ts",
    "apps/server/src/scheduledTasks/Schedule.ts",
    "apps/server/src/scheduledTasks/ScheduledTaskService.ts",
    "apps/web/src/components/ThreadNotificationCoordinator.badge.test.tsx",
    "apps/web/src/components/ThreadNotificationCoordinator.test.tsx",
    "apps/web/src/components/ThreadNotificationCoordinator.tsx",
    "apps/web/src/components/settings/ScheduledTasksSettings.tsx",
    "apps/web/src/components/settings/scheduledTasksSettings.logic.ts",
    "docs/user/project-settings.md",
    "packages/client-runtime/package.json",
    "packages/client-runtime/src/state/server.ts",
    "packages/contracts/src/scheduledTask.ts"
  ],
  "sharedFiles": [
    "apps/server/src/mcp/OrchestratorMcpService.ts",
    "apps/server/src/orchestration-v2/runtimeLayer.ts",
    "apps/server/src/persistence/forkV1Backfills.ts",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/orchestratorMcp.ts"
  ],
  "keywords": [
    "outcomeCheck",
    "checkCommand",
    "ScheduledTaskDispatchPolicy",
    "fork_scheduled_task_checks",
    "scheduler.state-set",
    "reportsFence"
  ]
}
```

## settings-mark

```json
{
  "id": "settings-mark",
  "purpose": "Mark settings that Chromeria adds so the user can tell them from upstream T3 settings.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/199"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/200"],
  "newFiles": ["apps/web/src/components/ChromeriaFeatureMark.tsx"],
  "upstreamFiles": [],
  "sharedFiles": ["apps/web/src/components/settings/SettingsSidebarNav.tsx"],
  "keywords": ["ChromeriaFeatureMark", "Chromeria feature"]
}
```

## thread-parent-crumbs

```json
{
  "id": "thread-parent-crumbs",
  "purpose": "Show a child thread's parent and siblings in its header.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/201"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/202"],
  "newFiles": [
    "apps/web/src/components/chat/ThreadParentCrumbs.tsx",
    "apps/web/src/components/chat/threadParentCrumbs.logic.test.ts",
    "apps/web/src/components/chat/threadParentCrumbs.logic.ts"
  ],
  "upstreamFiles": [],
  "sharedFiles": ["apps/web/src/components/chat/ChatHeader.tsx"],
  "keywords": ["ThreadParentCrumbs", "parent breadcrumb", "sibling threads"]
}
```
