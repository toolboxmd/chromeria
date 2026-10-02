# Fork feature map

This is the canonical feature inventory consumed by `scripts/fork-features.mjs`.
Each JSON block is one feature. Keep stable ids, a one-line purpose, Issue/PR
links, exact repository-relative paths and literal, case-insensitive watch keywords.
Empty file arrays mean no new files or no independently owned upstream edits.

`upstreamFiles` assigns every allowlisted path to exactly one primary feature.
`sharedFiles` records other features using that path; all three file arrays take
part in overlap detection. Primary ownership does not imply exclusive behavior.
Some historical allowlist entries (the threads toolkit) are actually fork-new
files; they remain allowlisted and have one owner for compatibility.

Update this map with each fork feature or deletion. The report is a heuristic:
renamed capabilities or different vocabulary still need human review. Keywords
flag possible overlap, not proof of duplication. See [the routine](fork.md#routine)
for per-match decisions and the absorption PR record.

## Project Direction

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

## Product glossary

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

## Chromeria branding

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
    "assets/chromeria/chromeria-icon-1024.png",
    "assets/chromeria/chromeria-web-apple-touch-180.png",
    "assets/chromeria/chromeria-web-favicon-16x16.png",
    "assets/chromeria/chromeria-web-favicon-32x32.png",
    "assets/chromeria/chromeria-web-favicon.ico",
    "assets/chromeria/chromeria-windows.ico",
    "apps/web/public/chromeria-mark.png"
  ],
  "upstreamFiles": [
    "apps/desktop/package.json",
    "apps/desktop/src/app/DesktopAppIdentity.test.ts",
    "apps/desktop/src/app/DesktopEnvironment.ts",
    "apps/desktop/src/app/DesktopPreReadyPlatform.test.ts",
    "scripts/build-desktop-artifact.ts",
    "scripts/build-desktop-artifact.test.ts",
    "scripts/lib/brand-assets.ts",
    "scripts/lib/brand-assets.test.ts",
    "apps/web/index.html",
    "apps/web/src/bootstrap.test.ts",
    "apps/web/src/branding.test.ts",
    "apps/web/src/branding.ts",
    "apps/web/src/bundledDev.test.ts",
    "apps/web/src/components/T3Wordmark.tsx",
    "apps/web/src/components/chat/MessagesTimeline.tsx",
    "apps/web/src/components/onboarding/WelcomeWizard.tsx",
    "apps/web/src/components/settings/IntegrationsSettings.tsx",
    "apps/web/src/components/settings/ThemePreviewCircles.tsx",
    "apps/web/src/components/sidebar/SidebarChrome.tsx",
    "apps/web/src/lib/bootError.ts",
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

## Child threads

```json
{
  "id": "child-threads",
  "purpose": "Spawn child or top-level threads in any project in the environment, report child work to the parent, and read, message or interrupt threads in the selected scope.",
  "issues": [
    "https://github.com/toolboxmd/t3code/issues/8",
    "https://github.com/toolboxmd/t3code/issues/48",
    "https://github.com/toolboxmd/t3code/issues/59",
    "https://github.com/toolboxmd/t3code/issues/61",
    "https://github.com/toolboxmd/chromeria/issues/95",
    "https://github.com/toolboxmd/chromeria/issues/94",
    "https://github.com/toolboxmd/chromeria/issues/71",
    "https://github.com/toolboxmd/chromeria/issues/114",
    "https://github.com/toolboxmd/chromeria/issues/122",
    "https://github.com/toolboxmd/chromeria/issues/118"
  ],
  "prs": [
    "https://github.com/toolboxmd/t3code/pull/10",
    "https://github.com/toolboxmd/t3code/pull/60",
    "https://github.com/toolboxmd/t3code/pull/63",
    "https://github.com/toolboxmd/chromeria/pull/102",
    "https://github.com/toolboxmd/chromeria/pull/103"
  ],
  "newFiles": [
    "apps/server/src/mcp/toolkits/threads/childReportState.test.ts",
    "apps/server/src/mcp/toolkits/threads/childReportState.ts",
    "apps/server/src/mcp/toolkits/threads/childThreads.test.ts",
    "apps/server/src/mcp/toolkits/threads/childUsageLimitResume.test.ts",
    "apps/server/src/mcp/toolkits/threads/handlers.testFixtures.ts",
    "apps/server/src/mcp/toolkits/threads/handlers.ts",
    "apps/server/src/mcp/toolkits/threads/interruptThread.test.ts",
    "apps/server/src/mcp/toolkits/threads/mobileShell.test.ts",
    "apps/server/src/mcp/toolkits/threads/mobileShell.ts",
    "apps/server/src/mcp/toolkits/threads/spawnIntoProject.test.ts",
    "apps/server/src/mcp/toolkits/threads/spawnSetup.ts",
    "apps/server/src/mcp/toolkits/threads/spawnWorkspace.ts",
    "apps/server/src/mcp/toolkits/threads/subagentThreadId.test.ts",
    "apps/server/src/mcp/toolkits/threads/subagentThreadId.ts",
    "apps/server/src/mcp/toolkits/threads/tools.ts",
    "apps/server/src/mcp/toolkits/threads/usageLimitResume.test.ts",
    "apps/server/src/mcp/toolkits/threads/usageLimitResume.ts",
    "apps/web/src/components/AgentThreadLink.tsx",
    "apps/web/src/components/subagentThreads.test.ts",
    "apps/web/src/components/subagentThreads.ts",
    "apps/server/src/mcp/toolkits/threads/retireSubtree.ts",
    "apps/server/src/mcp/toolkits/threads/retireSubtree.test.ts",
    "apps/server/src/orchestration/ThreadRetirement.ts",
    "apps/server/src/mcp/toolkits/threads/spectrum.ts",
    "apps/server/src/mcp/toolkits/threads/spectrum.test.ts",
    "apps/server/src/mcp/toolkits/threads/spectrumIdentity.ts",
    "apps/server/src/mcp/toolkits/threads/spectrumTools.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/entrypoint.test.ts",
    "apps/server/src/mcp/McpHttpServer.ts",
    "apps/server/src/orchestration/Layers/ProviderCommandReactor.test.ts",
    "apps/server/src/orchestration/Layers/ProviderCommandReactor.ts",
    "apps/server/src/orchestration/http.ts",
    "apps/server/src/server.test.ts",
    "apps/web/src/components/LegacySidebar.tsx",
    "apps/web/src/components/Sidebar.tsx",
    "apps/server/src/orchestration/Layers/OrchestrationEngine.ts",
    "apps/server/src/orchestration/Layers/ProjectionSnapshotQuery.ts",
    "apps/server/src/orchestration/Layers/ProjectionSnapshotQuery.test.ts",
    "apps/server/src/mcp/alwaysLoad.test.ts",
    "apps/server/src/orchestration/Services/OrchestrationEngine.ts",
    "apps/server/src/orchestration/Services/ProjectionSnapshotQuery.ts",
    "apps/server/src/project/AgentSessionImporter.test.ts",
    "apps/server/src/relay/AgentAwarenessRelay.test.ts",
    "apps/server/src/serverRuntimeStartup.reconcile.test.ts",
    "apps/server/src/serverRuntimeStartup.test.ts",
    "apps/server/src/serverRuntimeStartup.ts",
    "apps/server/src/serverRuntimeStartup.worktreeSetup.test.ts",
    "apps/server/src/orchestration/Layers/OrchestrationEngine.test.ts",
    "apps/server/src/orchestration/ActivityPayloadProjection.ts",
    "apps/server/src/orchestration/ActivityPayloadProjection.test.ts",
    "apps/web/src/components/ChatView.logic.ts",
    "apps/web/src/components/ChatView.logic.test.ts"
  ],
  "sharedFiles": [
    "apps/server/src/orchestration/decider.ts",
    "scripts/build-desktop-artifact.ts",
    "apps/web/src/components/AgentsPanel.tsx",
    "apps/server/src/ws.ts",
    "apps/web/src/components/ChatView.tsx",
    "apps/web/src/session-logic.ts",
    "apps/mobile/src/lib/threadActivity.ts"
  ],
  "keywords": [
    "parentThreadId",
    "child thread",
    "subagent",
    "spawn_thread",
    "interrupt_thread",
    "sidebar",
    "start_spectrum",
    "Spectrum",
    "Color",
    "council",
    "verbatim",
    "barrier"
  ]
}
```

## Fork maintenance and CI

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
    "docs/fork.md",
    "scripts/fork-check.sh",
    "scripts/fork-maintenance.test.ts",
    "scripts/fork-rebase.sh",
    "scripts/fork-upstream-edits.txt",
    "docs/fork-features.md",
    "scripts/fork-features.mjs"
  ],
  "upstreamFiles": [
    ".github/workflows/ci.yml",
    ".github/workflows/mobile-fingerprint-check.yml",
    "knip.jsonc",
    "AGENTS.md"
  ],
  "sharedFiles": ["scripts/build-desktop-artifact.ts", "apps/server/src/entrypoint.test.ts"],
  "keywords": ["fork", "rebase", "upstream", "blacksmith", "ELECTRON_RUN_AS_NODE", "TMPDIR"]
}
```

## Thread scope

```json
{
  "id": "thread-scope",
  "purpose": "Allow explicit same-project supervision while retaining child-only defaults.",
  "issues": ["https://github.com/toolboxmd/t3code/issues/15"],
  "prs": ["https://github.com/toolboxmd/t3code/pull/16"],
  "newFiles": [],
  "upstreamFiles": [
    "apps/server/src/mcp/toolkits/threads/childThreads.test.ts",
    "apps/server/src/mcp/toolkits/threads/handlers.ts",
    "apps/server/src/mcp/toolkits/threads/tools.ts"
  ],
  "sharedFiles": [],
  "keywords": ["thread scope", "scope", "projectId", "list_threads", "read_thread", "send_message"]
}
```

## Agents panel

```json
{
  "id": "agents-panel",
  "purpose": "Nest child threads under their parent and navigate parent and siblings.",
  "issues": ["https://github.com/toolboxmd/t3code/issues/17"],
  "prs": ["https://github.com/toolboxmd/t3code/pull/18"],
  "newFiles": [
    "apps/web/src/components/AgentThreadTree.logic.test.ts",
    "apps/web/src/components/AgentThreadTree.logic.ts",
    "apps/web/src/components/AgentThreadTree.tsx",
    "apps/web/src/components/chat/ThreadParentCrumbs.tsx"
  ],
  "upstreamFiles": [
    "apps/web/src/components/AgentsPanel.tsx",
    "apps/web/src/components/chat/ChatHeader.tsx"
  ],
  "sharedFiles": [],
  "keywords": [
    "AgentsPanel",
    "Direct Spawns",
    "child tree",
    "breadcrumb",
    "parentThreadId",
    "sidebar section"
  ]
}
```

## Prism toolkit and role kits

```json
{
  "id": "prism-toolkit",
  "purpose": "Give Prism roles their kits and models; every role has every thread tool.",
  "issues": [
    "https://github.com/toolboxmd/t3code/issues/19",
    "https://github.com/toolboxmd/chromeria/issues/93",
    "https://github.com/toolboxmd/chromeria/issues/115"
  ],
  "prs": [
    "https://github.com/toolboxmd/t3code/pull/22",
    "https://github.com/toolboxmd/chromeria/pull/101"
  ],
  "newFiles": [
    "apps/server/src/mcp/toolkits/threads/roles.test.ts",
    "apps/server/src/mcp/toolkits/threads/roles.ts",
    "apps/server/src/prism/promachosStart.ts",
    "packages/contracts/src/prism.test.ts",
    "packages/contracts/src/prism.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/provider/Drivers/OpenCodeDriver.ts",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/settings.ts",
    "packages/contracts/src/orchestration.ts"
  ],
  "sharedFiles": [
    "apps/server/src/mcp/toolkits/threads/handlers.ts",
    "apps/server/src/mcp/toolkits/threads/tools.ts",
    "apps/server/src/server.test.ts",
    "apps/server/src/ws.ts",
    "apps/web/src/components/settings/PrismSettings.tsx"
  ],
  "keywords": [
    "prism",
    "role kit",
    "prismRoles",
    "spawn_thread",
    "capacity",
    "usage limit",
    "resume"
  ]
}
```

## Prism settings page

```json
{
  "id": "prism-settings",
  "purpose": "Configure role preferences and show provider usage and capacity in Settings.",
  "issues": ["https://github.com/toolboxmd/t3code/issues/21"],
  "prs": ["https://github.com/toolboxmd/t3code/pull/23"],
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
    "apps/web/src/components/settings/settingsSearch.ts",
    "apps/web/src/routeTree.gen.ts"
  ],
  "sharedFiles": [],
  "keywords": ["PrismSettings", "prismRoles", "role preferences", "capacity", "usage", "settings"]
}
```

## Sidebar child-thread working state

```json
{
  "id": "sidebar-child-working",
  "purpose": "Show a parent thread as working, and keep it active, while its hidden child threads work.",
  "issues": ["https://github.com/toolboxmd/t3code/issues/31"],
  "prs": ["https://github.com/toolboxmd/t3code/pull/33"],
  "newFiles": [
    "apps/web/src/components/SidebarChildActivity.logic.test.ts",
    "apps/web/src/components/SidebarChildActivity.logic.ts",
    "packages/shared/src/childThreadActivity.test.ts",
    "packages/shared/src/childThreadActivity.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/orchestration/ThreadSettlementReactor.test.ts",
    "apps/server/src/orchestration/ThreadSettlementReactor.ts",
    "packages/shared/package.json"
  ],
  "sharedFiles": ["apps/web/src/components/Sidebar.tsx"],
  "keywords": ["backgroundLiveness", "child thread", "auto-settle", "Working", "agents working"]
}
```

## Browse Issues

```json
{
  "id": "issues-browse",
  "purpose": "List GitHub Issues of all project repositories beside PRs, with filters, parent tree and a side panel.",
  "issues": [
    "https://github.com/toolboxmd/t3code/issues/27",
    "https://github.com/toolboxmd/t3code/issues/42"
  ],
  "prs": ["https://github.com/toolboxmd/t3code/pull/32"],
  "newFiles": [
    "apps/server/src/issues/IssueService.live.test.ts",
    "apps/server/src/issues/IssueService.ts",
    "apps/server/src/issues/gitHubIssues.test.ts",
    "apps/server/src/issues/gitHubIssues.ts",
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
    "packages/contracts/src/issues.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/auth/RpcAuthorization.ts",
    "apps/server/src/pullRequest/GitHubPullRequestCli.ts",
    "apps/server/src/server.ts",
    "apps/server/src/ws.ts",
    "apps/web/src/components/CommandPalette.tsx",
    "apps/web/src/components/pullRequest/PullRequestListFilters.tsx",
    "apps/web/src/routes/_chat.pull-requests.tsx",
    "apps/web/src/state/pullRequests.ts",
    "docs/user/source-control.md",
    "packages/contracts/src/rpc.ts"
  ],
  "sharedFiles": [
    "apps/web/src/components/sidebar/SidebarChrome.tsx",
    "packages/contracts/src/index.ts"
  ],
  "keywords": [
    "issues",
    "sub-issue",
    "subIssues",
    "is:issue",
    "WsRpcGroup",
    "RPC_REQUIRED_SCOPES",
    "pull-requests route",
    "command palette"
  ]
}
```

## Issue links

```json
{
  "id": "issues-links",
  "purpose": "Link GitHub Issues to threads, show them beside PRs and start a linked thread from an Issue.",
  "issues": ["https://github.com/toolboxmd/t3code/issues/28"],
  "prs": ["https://github.com/toolboxmd/t3code/pull/34"],
  "newFiles": [
    "apps/server/src/issueLinks/IssueLinks.live.test.ts",
    "apps/server/src/issueLinks/IssueLinks.test.ts",
    "apps/server/src/issueLinks/IssueLinks.testFixtures.ts",
    "apps/server/src/issueLinks/IssueLinks.ts",
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
    "apps/web/src/state/issueLinks.ts",
    "packages/contracts/src/issueLinks.test.ts",
    "packages/contracts/src/issueLinks.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/environment/ServerEnvironment.ts",
    "apps/web/src/components/ChatView.tsx",
    "apps/web/src/components/RightPanelTabs.tsx",
    "apps/web/src/rightPanelStore.ts",
    "apps/web/src/rightPanelStore.test.ts",
    "packages/client-runtime/src/rpc/client.ts",
    "packages/contracts/src/environment.ts",
    "apps/web/src/components/pullRequest/ThreadPullRequestsPanel.tsx"
  ],
  "sharedFiles": [
    "apps/server/src/auth/RpcAuthorization.ts",
    "apps/server/src/mcp/McpHttpServer.ts",
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
    "Linked pull requests"
  ]
}
```

## Issue status

```json
{
  "id": "issues-status",
  "purpose": "Compute each Issue's status from GitHub, links and thread activity; group and filter the Issues view by it and wire linked threads and Start thread.",
  "issues": ["https://github.com/toolboxmd/t3code/issues/29"],
  "prs": ["https://github.com/toolboxmd/t3code/pull/36"],
  "newFiles": [
    "apps/server/src/issues/issueStatus.live.test.ts",
    "apps/server/src/issues/reviewMark.test.ts",
    "packages/contracts/src/issueStatus.test.ts",
    "packages/contracts/src/issueStatus.ts",
    "apps/web/src/components/issues/issuePaletteThreads.logic.test.ts",
    "apps/web/src/components/issues/issuePaletteThreads.logic.ts",
    "apps/web/src/components/issues/issuePaletteThreads.ts",
    "apps/web/src/components/issues/issueStatus.logic.test.ts",
    "apps/web/src/components/issues/issueStatus.logic.ts",
    "apps/web/src/components/issues/useIssueRowThreads.ts"
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
    "pull-requests route"
  ]
}
```

## Issue and pull request links open in the app

```json
{
  "id": "issues-open-links",
  "purpose": "Open GitHub /issues/N links and an Issue's pull requests in the app: the PR panel for pull requests, the Issues panel for Issues, the browser otherwise.",
  "issues": ["https://github.com/toolboxmd/t3code/issues/40"],
  "prs": ["https://github.com/toolboxmd/t3code/pull/41"],
  "newFiles": [
    "apps/web/src/components/issues/issueLinkOpening.logic.test.ts",
    "apps/web/src/components/issues/issueLinkOpening.logic.ts",
    "apps/web/src/components/issues/useOpenIssueOrPullRequestLink.ts"
  ],
  "upstreamFiles": [
    "apps/web/src/components/ChatMarkdown.tsx",
    "apps/web/src/lib/openPullRequestLink.ts"
  ],
  "sharedFiles": ["docs/user/source-control.md"],
  "keywords": [
    "pullRequestCandidateUrlFromReferenceAutolink",
    "useOpenChangeRequestLink",
    "PullRequestLinkPreview",
    "MarkdownAnchor"
  ]
}
```

## Interrupted environment reads are read again

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

## Tool instructions for agents

```json
{
  "id": "tool-instructions",
  "purpose": "Tell agents when to reach for the t3-code tools and how to route delegated work with spawn_thread roles, even when harnesses defer tool schemas.",
  "issues": ["https://github.com/toolboxmd/t3code/issues/46"],
  "prs": ["https://github.com/toolboxmd/t3code/pull/53"],
  "newFiles": [
    "apps/server/src/mcp/toolInstructions.test.ts",
    "apps/server/src/mcp/toolInstructions.ts"
  ],
  "upstreamFiles": ["apps/server/src/provider/RuntimeInstructions.ts"],
  "sharedFiles": [],
  "keywords": [
    "t3_code_tool_use",
    "buildRuntimeInstructions",
    "McpServer.layerHttp",
    "instructions:"
  ]
}
```

## Prism stream clock

```json
{
  "id": "prism-stream-clock",
  "purpose": "Stamp each thread's last provider stream event in memory for stale-turn detection and record per-turn stream statistics.",
  "issues": ["https://github.com/toolboxmd/t3code/issues/55"],
  "prs": ["https://github.com/toolboxmd/t3code/pull/56"],
  "newFiles": [
    "apps/server/src/prism/streamClock.test.ts",
    "apps/server/src/prism/streamClock.ts",
    "apps/server/src/prism/streamStats.ts"
  ],
  "upstreamFiles": ["apps/mobile/src/lib/threadActivity.ts", "apps/web/src/session-logic.ts"],
  "sharedFiles": [
    "apps/server/src/mcp/McpHttpServer.ts",
    "apps/server/src/server.ts",
    "packages/contracts/src/prism.ts"
  ],
  "keywords": [
    "stream clock",
    "liveness",
    "ProviderEventLoggers",
    "thinking_tokens",
    "prism.stream-stats",
    "context-window.updated"
  ]
}
```

## Stale-turn detector

```json
{
  "id": "prism-stale-turn-detector",
  "purpose": "Detect silent or dead worker turns from the stream clock and measured healthy gaps, exclude host sleep, and notify the spawning parent once.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/62"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/65"],
  "newFiles": [
    "apps/server/src/prism/staleTurnDetector.ts",
    "apps/server/src/prism/staleTurnDetector.test.ts",
    "apps/server/src/prism/staleTurnMonitor.ts",
    "apps/server/src/prism/staleTurnMonitor.test.ts"
  ],
  "upstreamFiles": [],
  "sharedFiles": [
    "apps/server/src/mcp/McpHttpServer.ts",
    "apps/server/src/prism/streamClock.ts",
    "apps/server/src/prism/streamClock.test.ts",
    "apps/server/src/prism/streamStats.ts",
    "packages/contracts/src/prism.ts"
  ],
  "keywords": [
    "stale",
    "liveness",
    "stream clock",
    "prism.stream-stats",
    "spawn_thread",
    "host sleep"
  ]
}
```

## Mermaid diagrams as images

```json
{
  "id": "mermaid-diagram-images",
  "purpose": "Render mermaid code fences in chat as diagram images, falling back to the code block when a diagram is invalid (upstream pingdotgg/t3code#13970).",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/90"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/91"],
  "newFiles": ["apps/web/src/lib/mermaid.test.ts", "apps/web/src/lib/mermaid.ts"],
  "upstreamFiles": [
    "apps/web/package.json",
    "apps/web/src/components/ChatMarkdown.test.tsx",
    "pnpm-lock.yaml",
    "third-party-licenses.config.json"
  ],
  "sharedFiles": ["apps/web/src/components/ChatMarkdown.tsx"],
  "keywords": ["mermaid", "renderMermaidImage", "MermaidDiagram"]
}
```

## Promachos mode

```json
{
  "id": "promachos-mode",
  "purpose": "Switch web and desktop to a chat-first view of the Promachos home's conversations: paragraph bubbles, a working indicator, inline question and approval cards, and new conversations started on his Prism role.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/116"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/128"],
  "newFiles": [
    "apps/web/src/components/promachos/PromachosChat.test.tsx",
    "apps/web/src/components/promachos/PromachosChat.tsx",
    "apps/web/src/components/promachos/PromachosModeSwitch.tsx",
    "apps/web/src/components/promachos/PromachosSidebar.tsx",
    "apps/web/src/components/promachos/promachosBubbles.test.tsx",
    "apps/web/src/components/promachos/promachosBubbles.ts",
    "apps/web/src/components/promachos/promachosConversations.test.ts",
    "apps/web/src/components/promachos/promachosConversations.ts",
    "apps/web/src/components/promachos/promachosMode.ts",
    "apps/web/src/components/promachos/promachosStart.test.ts",
    "apps/web/src/components/promachos/promachosStart.ts",
    "apps/web/src/components/promachos/promachosTimeline.ts",
    "docs/user/promachos-mode.md"
  ],
  "upstreamFiles": [
    "apps/web/src/components/AppSidebarLayout.tsx",
    "apps/web/src/components/chat/ChatComposer.tsx",
    "docs/README.md"
  ],
  "sharedFiles": [
    "apps/web/src/components/ChatView.tsx",
    "apps/web/src/components/chat/MessagesTimeline.tsx",
    "apps/web/src/components/sidebar/SidebarChrome.tsx"
  ],
  "keywords": ["Promachos", "chat bubbles", "bubble", "chat-first", "inline approval"]
}
```

## Wight mode

```json
{
  "id": "wight-mode",
  "purpose": "Continue idle threads within a timer and provider-instance usage limit.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/117"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/127"],
  "newFiles": [
    "apps/server/src/mcp/toolkits/threads/wightAdmission.test.ts",
    "apps/server/src/mcp/toolkits/threads/wightMode.test.ts",
    "apps/server/src/mcp/toolkits/threads/wightToolkit.test.ts",
    "apps/server/src/mcp/toolkits/threads/wightMode.ts",
    "apps/server/src/mcp/toolkits/threads/sendThreadTurn.ts",
    "apps/web/src/components/chat/WightModeControl.tsx",
    "apps/web/src/components/settings/WightLimitSetting.tsx",
    "apps/web/src/wightMode.test.ts",
    "apps/web/src/wightMode.ts",
    "packages/contracts/src/wight.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/orchestration/decider.ts",
    "apps/web/src/components/settings/ProviderInstanceCard.tsx",
    "docs/user/thread-sidebar.md",
    "packages/contracts/src/providerInstance.ts",
    "packages/shared/src/serverSettings.ts"
  ],
  "sharedFiles": [
    "apps/server/src/mcp/toolkits/threads/childUsageLimitResume.test.ts",
    "apps/server/src/mcp/toolkits/threads/handlers.testFixtures.ts",
    "apps/server/src/mcp/toolkits/threads/handlers.ts",
    "apps/web/src/components/chat/ChatHeader.tsx",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/settings.ts"
  ],
  "keywords": ["Wight", "idle continuation", "wightLimitPercent", "wightModes"]
}
```

## Device people and thread ownership

```json
{
  "id": "thread-people",
  "purpose": "Label devices, filter threads by person and share threads, as views without access control.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/121"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/131"],
  "newFiles": [
    "apps/server/src/orchestration/stampCommandPerson.ts",
    "apps/server/src/orchestration/people.test.ts",
    "apps/server/src/persistence/forkThreadPeopleSchema.test.ts",
    "apps/server/src/persistence/forkThreadPeopleSchema.ts",
    "apps/web/src/components/people/ClientPersonSelect.tsx",
    "apps/web/src/components/people/PersonPicker.tsx",
    "apps/web/src/components/people/SharedThreadLabel.tsx",
    "apps/web/src/components/people/ThreadSharingControl.tsx",
    "apps/web/src/components/people/personView.test.ts",
    "apps/web/src/components/people/personView.ts",
    "apps/web/src/components/people/sharingTimeline.test.ts",
    "apps/web/src/components/people/threadSharing.ts",
    "apps/web/src/components/people/usePersonView.ts",
    "docs/user/people.md",
    "packages/contracts/src/people.ts"
  ],
  "upstreamFiles": [
    "apps/mobile/src/connection/environment-cache-store.test.ts",
    "apps/mobile/src/features/archive/archivedThreadList.test.ts",
    "apps/mobile/src/features/home/homeThreadList.test.ts",
    "apps/mobile/src/features/threads/threadListV2.test.ts",
    "apps/mobile/src/lib/threadActivity.test.ts",
    "apps/mobile/src/state/pending-thread-creation.ts",
    "apps/mobile/src/state/use-thread-selection.ts",
    "apps/server/src/auth/EnvironmentAuth.test.ts",
    "apps/server/src/auth/EnvironmentAuth.ts",
    "apps/server/src/auth/SessionStore.test.ts",
    "apps/server/src/auth/SessionStore.ts",
    "apps/server/src/auth/http.ts",
    "apps/server/src/git/linkCreatedPullRequest.test.ts",
    "apps/server/src/mcp/toolkits/pullRequests/handlers.test.ts",
    "apps/server/src/orchestration/Layers/ProjectionPipeline.ts",
    "apps/server/src/orchestration/PullRequestSyncReactor.test.ts",
    "apps/server/src/orchestration/Schemas.ts",
    "apps/server/src/orchestration/ThreadPullRequestReactor.test.ts",
    "apps/server/src/orchestration/ThreadSettlementPolicy.test.ts",
    "apps/server/src/orchestration/commandInvariants.test.ts",
    "apps/server/src/orchestration/decider.active-order.test.ts",
    "apps/server/src/orchestration/decider.autoSettleSet.test.ts",
    "apps/server/src/orchestration/decider.pinned.test.ts",
    "apps/server/src/orchestration/decider.pullRequests.test.ts",
    "apps/server/src/orchestration/decider.questionAttachments.test.ts",
    "apps/server/src/orchestration/decider.settled.test.ts",
    "apps/server/src/orchestration/decider.snoozed.test.ts",
    "apps/server/src/orchestration/decider.titleRegeneration.test.ts",
    "apps/server/src/orchestration/decider.turnDiffComplete.test.ts",
    "apps/server/src/orchestration/decider.userInputDismiss.test.ts",
    "apps/server/src/orchestration/messageContext.test.ts",
    "apps/server/src/orchestration/projector.test.ts",
    "apps/server/src/orchestration/projector.ts",
    "apps/server/src/persistence/AuthSessions.ts",
    "apps/server/src/persistence/Layers/ProjectionThreads.ts",
    "apps/server/src/persistence/Layers/Sqlite.ts",
    "apps/server/src/persistence/Services/ProjectionThreads.ts",
    "apps/server/src/provider/Layers/ProviderSessionReaper.test.ts",
    "apps/web/src/components/CommandPalette.logic.test.ts",
    "apps/web/src/components/Sidebar.logic.test.ts",
    "apps/web/src/components/chat/MessagesTimeline.logic.test.ts",
    "apps/web/src/components/chat/MessagesTimeline.logic.ts",
    "apps/web/src/components/settings/ConnectionsSettings.tsx",
    "apps/web/src/environments/primary/auth.ts",
    "apps/web/src/environments/primary/index.ts",
    "apps/web/src/lib/threadSort.test.ts",
    "apps/web/src/state/threads.test.ts",
    "apps/web/src/worktreeCleanup.test.ts",
    "apps/web/test/environmentHttpTest.ts",
    "packages/client-runtime/src/remotePerformance.bench.ts",
    "packages/client-runtime/src/state/entities.test.ts",
    "packages/client-runtime/src/state/environmentHttpAuth.test.ts",
    "packages/client-runtime/src/state/shellReducer.test.ts",
    "packages/client-runtime/src/state/threadCommands.test.ts",
    "packages/client-runtime/src/state/threadReducer.test.ts",
    "packages/client-runtime/src/state/threadReducer.ts",
    "packages/client-runtime/src/state/threads-atoms.test.ts",
    "packages/client-runtime/src/state/threads-pagination.test.ts",
    "packages/client-runtime/src/state/threads-sync.test.ts",
    "packages/contracts/src/auth.ts",
    "packages/contracts/src/environmentHttp.ts"
  ],
  "sharedFiles": [
    "apps/server/src/orchestration/http.ts",
    "apps/server/src/mcp/toolkits/threads/handlers.ts",
    "apps/server/src/orchestration/Layers/OrchestrationEngine.test.ts",
    "apps/server/src/orchestration/Layers/ProjectionSnapshotQuery.test.ts",
    "apps/server/src/orchestration/Layers/ProjectionSnapshotQuery.ts",
    "apps/server/src/orchestration/ThreadSettlementReactor.test.ts",
    "apps/server/src/orchestration/decider.ts",
    "apps/server/src/project/AgentSessionImporter.test.ts",
    "apps/server/src/relay/AgentAwarenessRelay.test.ts",
    "apps/server/src/server.test.ts",
    "apps/server/src/ws.ts",
    "apps/web/src/components/ChatView.logic.test.ts",
    "apps/web/src/components/ChatView.logic.ts",
    "apps/web/src/components/Sidebar.tsx",
    "apps/web/src/components/SidebarChildActivity.logic.test.ts",
    "apps/web/src/components/chat/ChatHeader.tsx",
    "apps/web/src/components/issues/issueStatus.logic.test.ts",
    "apps/web/src/components/promachos/PromachosSidebar.tsx",
    "apps/web/src/components/promachos/promachosTimeline.ts",
    "apps/web/src/components/sidebar/SidebarChrome.tsx",
    "docs/README.md",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/orchestration.ts"
  ],
  "keywords": ["thread ownership", "coOwners", "person picker"]
}
```

## Check-gated scheduled tasks

```json
{
  "id": "scheduled-tasks",
  "purpose": "Run repeating and one-shot tasks until their pinned server outcome check passes, with durable same-thread recovery.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/123"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/132"],
  "newFiles": [
    "apps/server/src/mcp/toolkits/scheduler/handlers.ts",
    "apps/server/src/mcp/toolkits/scheduler/tools.ts",
    "apps/server/src/scheduler/RunReports.ts",
    "apps/server/src/scheduler/SpectrumIntegration.test.ts",
    "apps/server/src/scheduler/Schedule.test.ts",
    "apps/server/src/scheduler/Schedule.ts",
    "apps/server/src/scheduler/Scheduler.test.ts",
    "apps/server/src/scheduler/Scheduler.ts",
    "apps/server/src/scheduler/Service.test.ts",
    "apps/server/src/scheduler/Service.ts",
    "apps/server/src/scheduler/rpcHandlers.ts",
    "apps/web/src/components/settings/ScheduledTasksSettings.logic.test.ts",
    "apps/web/src/components/settings/ScheduledTasksSettings.logic.ts",
    "apps/web/src/components/settings/ScheduledTasksSettings.tsx",
    "apps/web/src/routes/settings.scheduled-tasks.tsx",
    "apps/web/src/state/scheduler.ts",
    "docs/user/scheduled-tasks.md",
    "packages/contracts/src/scheduler.ts"
  ],
  "upstreamFiles": [],
  "sharedFiles": [
    "apps/server/src/orchestration/projector.ts",
    "apps/server/src/auth/RpcAuthorization.ts",
    "apps/server/src/mcp/McpHttpServer.ts",
    "apps/server/src/mcp/toolkits/threads/handlers.ts",
    "apps/server/src/mcp/toolkits/threads/spectrum.ts",
    "apps/server/src/orchestration/Layers/OrchestrationEngine.ts",
    "apps/server/src/orchestration/Services/OrchestrationEngine.ts",
    "apps/server/src/orchestration/decider.ts",
    "apps/server/src/server.test.ts",
    "apps/server/src/server.ts",
    "apps/server/src/ws.ts",
    "apps/web/src/components/settings/SettingsSidebarNav.tsx",
    "apps/web/src/components/settings/settingsSearch.ts",
    "apps/web/src/routeTree.gen.ts",
    "docs/README.md",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/orchestration.ts",
    "packages/contracts/src/rpc.ts"
  ],
  "keywords": ["scheduler", "scheduledTasks", "scheduler.state-set", "schedulerOwnsThread"]
}
```

## Partial-clone remotes keep repository identity

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
    "apps/server/src/vcs/GitVcsDriverCore.test.ts",
    "apps/server/src/vcs/GitVcsDriverCore.ts"
  ],
  "sharedFiles": [],
  "keywords": ["partial clone", "blob:none", "partialclonefilter", "git remote -v"]
}
```
