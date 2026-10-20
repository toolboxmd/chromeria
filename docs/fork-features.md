# Fork carried-edit registry

Only the carried features below and the V2 data foundation belong to this integration stack.
Each JSON entry assigns actual carried edits one primary owner. Shared paths are
watched by other carried features; deferred features are excluded. Keyword matches
are evidence for review, not proof that a feature has been adopted upstream.

The [fork maintenance guide](fork.md) explains absorption and proof. `prs` lists
PRs that introduced the current carried file set, or compare URLs. Add a later PR
only when that file set changes; this is not an exhaustive history of follow-ups.

## spectrum

```json
{
  "id": "spectrum",
  "purpose": "Run council and free Spectrum conversations with durable server-authored transcripts and exact-run barriers.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/176",
    "https://github.com/toolboxmd/chromeria/issues/396",
    "https://github.com/toolboxmd/chromeria/issues/401",
    "https://github.com/toolboxmd/chromeria/issues/404"
  ],
  "prs": [
    "https://github.com/toolboxmd/chromeria/compare/fork/v2...feat/176-spectrum-v2",
    "https://github.com/toolboxmd/chromeria/pull/494",
    "https://github.com/toolboxmd/chromeria/pull/508",
    "https://github.com/toolboxmd/chromeria/pull/514"
  ],
  "newFiles": [
    "apps/mobile/src/features/settings/spectrumReportAction.tsx",
    "apps/server/src/fork/ForkCommandInterceptor.ts",
    "apps/server/src/fork/ForkProviderlessMessages.ts",
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
    "apps/server/src/spectrum/delegatedTask.integration.test.ts",
    "apps/server/src/spectrum/delegatedTask.ts",
    "apps/server/src/spectrum/freeMode.ts",
    "apps/server/src/spectrum/freeMode.test.ts",
    "apps/server/src/spectrum/freeMode.integration.test.ts",
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
    "apps/server/src/spectrum/wakes.integration.test.ts",
    "apps/server/src/spectrum/wakes.ts",
    "apps/web/src/components/settings/spectrumReportAction.tsx",
    "apps/web/src/spectrumStop.ts",
    "apps/web/src/spectrumStop.test.ts",
    "packages/client-runtime/src/spectrumReport.test.ts",
    "packages/client-runtime/src/spectrumReport.ts",
    "packages/client-runtime/src/spectrumStop.ts",
    "packages/client-runtime/src/spectrumStop.test.ts",
    "packages/contracts/src/spectrum.ts",
    "packages/contracts/src/spectrumRpc.test.ts",
    "packages/contracts/src/spectrumRpc.ts",
    "apps/server/src/spectrum/settings.integration.test.ts",
    "apps/server/src/spectrum/settings.test.ts",
    "apps/server/src/spectrum/settings.ts",
    "apps/web/src/components/settings/SpectrumSettings.logic.test.ts",
    "apps/web/src/components/settings/SpectrumSettings.logic.ts",
    "apps/web/src/components/settings/SpectrumSettings.tsx",
    "apps/web/src/components/settings/SpectrumSettings.test.tsx",
    "apps/web/src/routes/settings.spectrum.tsx",
    "packages/contracts/src/spectrumSettings.test.ts",
    "packages/contracts/src/spectrumSettings.ts"
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
    "apps/server/src/orchestration-v2/ThreadManagementService.ts",
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
    "packages/contracts/src/rpc.ts",
    "GLOSSARY.md",
    "apps/web/src/components/settings/ColorList.tsx",
    "apps/web/src/components/settings/SettingsSidebarNav.tsx",
    "apps/web/src/components/settings/settingsSearch.test.ts",
    "apps/web/src/components/settings/settingsSearch.ts",
    "apps/web/src/routeTree.gen.ts",
    "docs/map.md",
    "packages/contracts/src/settings.ts"
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
  "issues": [
    "https://github.com/toolboxmd/t3code/issues/11",
    "https://github.com/toolboxmd/chromeria/issues/218"
  ],
  "prs": ["https://github.com/toolboxmd/t3code/pull/11"],
  "newFiles": ["GLOSSARY.md"],
  "upstreamFiles": [],
  "sharedFiles": ["AGENTS.md"],
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
    "apps/mobile/chromeria.test.ts",
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
    "apps/mobile/app.config.ts",
    "apps/desktop/package.json",
    "apps/desktop/src/app/DesktopAppIdentity.test.ts",
    "apps/desktop/src/app/DesktopAppIdentity.ts",
    "apps/desktop/src/window/DesktopApplicationMenu.test.ts",
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
    "apps/web/src/components/chat/MessagesTimeline.browser.test.tsx",
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
  "purpose": "Keep the fork stack small, checked and rebasable on GitHub-hosted CI, including required upstream compatibility repairs.",
  "issues": [
    "https://github.com/toolboxmd/t3code/issues/6",
    "https://github.com/toolboxmd/t3code/issues/20",
    "https://github.com/toolboxmd/chromeria/issues/244",
    "https://github.com/toolboxmd/chromeria/issues/308",
    "https://github.com/toolboxmd/chromeria/issues/287",
    "https://github.com/toolboxmd/chromeria/issues/370",
    "https://github.com/toolboxmd/chromeria/issues/411",
    "https://github.com/toolboxmd/chromeria/issues/380"
  ],
  "prs": [
    "https://github.com/toolboxmd/t3code/pull/9",
    "https://github.com/toolboxmd/t3code/pull/24",
    "https://github.com/toolboxmd/chromeria/compare/main...fix/244-acp-replay-atomic-status",
    "https://github.com/toolboxmd/chromeria/pull/309",
    "https://github.com/toolboxmd/chromeria/compare/main...fix/287-dev-runner-tailscale-serve",
    "https://github.com/toolboxmd/chromeria/pull/373",
    "https://github.com/toolboxmd/chromeria/pull/419",
    "https://github.com/toolboxmd/chromeria/pull/391"
  ],
  "newFiles": [
    ".github/workflows/fork.yml",
    "docs/fork-features.md",
    "docs/fork.md",
    "scripts/fork-check.sh",
    "scripts/fork-features.mjs",
    "scripts/fork-maintenance.test.ts",
    "scripts/fork-rebase.sh",
    "scripts/fork-upstream-edits.txt",
    "patches/vitest@5.0.1.patch",
    "scripts/lib/vitest-cache.test.ts",
    "apps/server/src/device/DeviceInventory.test.ts",
    "apps/web/src/components/device/DeviceDiscovery.browser.test.tsx",
    "apps/server/src/forkWsRpc.ts"
  ],
  "upstreamFiles": [
    ".github/workflows/ci.yml",
    ".github/workflows/pr-size.yml",
    ".github/workflows/pr-vouch.yml",
    ".github/workflows/mobile-fingerprint-check.yml",
    "AGENTS.md",
    "apps/server/scripts/acp-replay-agent.ts",
    "apps/server/src/auth/PairingGrantStore.test.ts",
    "apps/server/src/device/DeviceService.ts",
    "apps/server/src/device/LocalDeviceHost.test.ts",
    "apps/server/src/device/LocalDeviceHost.ts",
    "apps/server/src/provider/acp/AcpSessionRuntime.processTree.test.ts",
    "apps/server/src/testUtils/shardWeights.json",
    "apps/server/src/orchestration-v2/V1ImportBoundary.test.ts",
    "apps/server/src/vcs/GitVcsDriverCore.test.ts",
    "apps/web/src/components/device/DevicePanel.tsx",
    "apps/web/src/components/device/DeviceHostUpdates.tsx",
    "apps/web/src/components/device/DeviceSetup.test.ts",
    "apps/web/src/components/device/DeviceSetup.tsx",
    "knip.jsonc",
    "scripts/dev-runner.ts",
    "scripts/dev-runner.test.ts",
    "pnpm-lock.yaml",
    "pnpm-workspace.yaml",
    "packages/contracts/src/device.ts",
    "packages/source-control-github/graphql.config.ts",
    "packages/source-control-github/src/server/GitHubPullRequestApi.test.ts",
    "packages/source-control-github/src/server/GitHubPullRequestApi.ts",
    "packages/source-control-github/src/server/gitHubPullRequestJson.test.ts",
    "packages/source-control-github/src/server/gitHubPullRequestJson.ts",
    "packages/source-control-github/src/server/pullRequestBatches.graphql",
    "packages/source-control-github/src/server/pullRequestBatches.graphql.ts",
    "packages/source-control-github/src/server/pullRequestListings.graphql",
    "packages/source-control-github/src/server/pullRequestListings.graphql.ts"
  ],
  "sharedFiles": [],
  "keywords": [
    "fork",
    "rebase",
    "upstream",
    "blacksmith",
    "ELECTRON_RUN_AS_NODE",
    "TMPDIR",
    "vitest",
    "JavaScriptRepresentable",
    "expo-modules-jsi"
  ]
}
```

## claude-steer-queue

```json
{
  "id": "claude-steer-queue",
  "purpose": "Queue routine agent steers into Claude instead of cancelling running tools and reporting that as a user refusal.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/280"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/282"],
  "newFiles": [
    "apps/server/src/orchestration-v2/testkit/fixtures/claude_queued_steer/claude_transcript.ndjson",
    "apps/server/src/orchestration-v2/testkit/fixtures/claude_queued_steer/input.ts",
    "apps/server/src/orchestration-v2/testkit/fixtures/claude_queued_steer/output.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/orchestration-v2/ClaudeAutomaticDelivery.integration.test.ts",
    "apps/server/src/orchestration-v2/CommandPolicy.test.ts",
    "apps/server/src/orchestration-v2/CommandPolicy.ts",
    "apps/server/src/orchestration-v2/testkit/fixtures/message_steering/claude_output.ts",
    "apps/server/src/orchestration-v2/testkit/fixtures/shared.ts",
    "docs/orchestration-v2/orchestrator-mcp-server.md"
  ],
  "sharedFiles": [
    "apps/server/src/mcp/toolkits/orchestrator/tools.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "apps/server/src/orchestration-v2/testkit/fixtures/index.ts",
    "packages/client-runtime/src/operations/commands.ts"
  ],
  "keywords": ["activeSteeringInterruptsTools", "steeringCancelsRunningTools", "queue_after_active"]
}
```

## urgent-steering-interruption

```json
{
  "id": "urgent-steering-interruption",
  "purpose": "Use Stop's native scope for urgent agent restart or human Steer, while routine Claude updates queue and interruption context names the requester.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/285",
    "https://github.com/toolboxmd/chromeria/issues/298"
  ],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/300",
    "https://github.com/toolboxmd/chromeria/compare/main...fix/298-restart-retry-receipt"
  ],
  "newFiles": [
    "apps/server/src/orchestration-v2/SteeringInterruption.ts",
    "apps/server/src/orchestration-v2/SteeringInterruption.test.ts",
    "apps/server/src/orchestration-v2/UrgentRestart.integration.test.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/orchestration-v2/SelectionRestart.integration.test.ts",
    "apps/server/src/orchestration-v2/ProviderTurnControlService.ts",
    "apps/server/src/orchestration-v2/RunExecutionService.test.ts",
    "apps/server/src/orchestration-v2/SteeringCompletion.integration.test.ts",
    "docs/user/composer.md",
    "packages/provider-opencode/src/server/v2/adapter.ts",
    "apps/server/src/orchestration-v2/Adapters/OpenCode2AdapterV2.test.ts",
    "apps/server/src/orchestration-v2/ProviderEventIngestor.test.ts",
    "apps/server/src/orchestration-v2/testkit/OrchestratorReplayRestartBackgroundNote.integration.test.ts",
    "apps/server/src/orchestration-v2/testkit/OrchestratorReplayRecovery.integration.test.ts"
  ],
  "sharedFiles": [
    "packages/provider-opencode/src/server/adapter.test.ts",
    "packages/provider-opencode/src/server/adapter.ts",
    "apps/server/src/orchestration-v2/Adapters/CodexAdapterV2.ts",
    "apps/server/src/orchestration-v2/Adapters/CodexAdapterV2.test.ts",
    "apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.test.ts",
    "packages/provider-grok/src/testing.ts",
    "apps/server/src/mcp/toolkits/orchestrator/tools.ts",
    "apps/server/src/mcp/toolkits/thread/handlers.ts",
    "apps/server/src/mcp/toolkits/thread/tools.ts",
    "apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.ts",
    "packages/provider-acp/src/server/adapter.ts",
    "packages/provider-grok/src/server/adapter.ts",
    "apps/server/src/orchestration-v2/ClaudeAutomaticDelivery.integration.test.ts",
    "apps/server/src/orchestration-v2/CommandPolicy.test.ts",
    "apps/server/src/orchestration-v2/CommandPolicy.ts",
    "apps/server/src/orchestration-v2/EffectOutbox.ts",
    "apps/server/src/orchestration-v2/EffectWorker.test.ts",
    "apps/server/src/orchestration-v2/EffectWorker.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "apps/server/src/orchestration-v2/RunExecutionService.ts",
    "apps/server/src/orchestration-v2/ThreadManagementService.test.ts",
    "apps/server/src/orchestration-v2/ThreadManagementService.ts",
    "packages/provider-core/src/server/orchestrationInstructions.ts",
    "apps/server/src/ws.ts",
    "docs/orchestration-v2/orchestrator-mcp-server.md",
    "docs/map.md",
    "packages/contracts/src/orchestrationV2.ts",
    "apps/server/src/orchestration-v2/ProviderTurnControlService.test.ts",
    "packages/provider-core/src/server/ProviderAdapter.ts",
    "apps/server/src/orchestration-v2/ProviderTurnStartService.ts",
    "apps/server/src/orchestration-v2/ProviderTurnStartService.bare.test.ts"
  ],
  "keywords": [
    "SteeringInterruption",
    "requestedBy",
    "interruption",
    "backgroundWork",
    "restart_active",
    "replacementFollows"
  ]
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
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/159",
    "https://github.com/toolboxmd/chromeria/issues/266",
    "https://github.com/toolboxmd/chromeria/issues/263",
    "https://github.com/toolboxmd/chromeria/issues/276"
  ],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/160"],
  "newFiles": [
    "apps/server/src/provider/forkProviderMaintenance.ts",
    "apps/server/src/provider/forkProviderStartAdmission.ts",
    "apps/server/src/provider/providerAdmissionGate.test.ts",
    "apps/server/src/provider/providerAdmissionGate.ts",
    "apps/server/src/provider/providerAutoUpdate.test.ts",
    "apps/server/src/provider/providerAutoUpdate.ts",
    "apps/server/src/provider/providerAutoUpdateState.ts",
    "apps/web/src/components/settings/AutoUpdateProvidersSetting.tsx",
    "apps/web/src/components/settings/ProviderUpdateSettingsRows.tsx",
    "apps/web/src/components/settings/providerUpdateSettingsSearch.test.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/orchestration-v2/ProviderSessionManager.test.ts",
    "apps/server/src/orchestration-v2/ProviderSessionManager.ts",
    "packages/provider-grok/src/server/driver.test.ts",
    "packages/provider-grok/src/server/driver.ts",
    "packages/provider-core/src/server/maintenance.ts",
    "packages/provider-core/src/server/maintenanceResolver.test.ts",
    "packages/provider-core/src/server/maintenanceResolver.ts",
    "apps/server/src/provider/providerMaintenanceRunner.test.ts",
    "apps/server/src/provider/providerMaintenanceRunner.ts",
    "apps/server/src/server.ts",
    "apps/server/src/ws.ts",
    "apps/web/src/components/settings/ProviderSettingsPanel.tsx",
    "apps/web/src/components/settings/ProviderSettingsPanel.environment.browser.test.tsx",
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
  "purpose": "Isolate the Chromeria V2 database, retain passive legacy tables and deferred settings data and provide shell-only backfills and fork-origin PR lookup.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/167"],
  "prs": ["https://github.com/toolboxmd/chromeria/compare/fork/v2...port/167-foundation"],
  "newFiles": [
    "apps/server/src/persistence/forkV1Backfills.test.ts",
    "apps/server/src/persistence/forkV1Backfills.ts",
    "apps/server/src/persistence/forkV1Snapshot.testFixtures.ts",
    "apps/server/src/pullRequest/forkRepositoryIdentity.ts",
    "packages/contracts/src/forkSettings.ts",
    "apps/server/src/persistence/forkV1Migrations.ts"
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
    "packages/contracts/src/settings.ts",
    "apps/server/src/persistence/Migrations/063_DropUnusedV1Projections.ts",
    "apps/server/src/persistence/Migrations/063_DropUnusedV1Projections.test.ts"
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
    "packages/contracts/src/issues.ts",
    "apps/server/src/issues/IssueGitHubApi.ts",
    "apps/server/src/issues/IssueGitHubApi.test.ts",
    "apps/server/src/issues/gitHubDocuments.ts"
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
    "packages/contracts/src/rpc.ts",
    "packages/source-control-github/package.json",
    "apps/server/src/sourceControl/builtInDrivers.ts",
    "apps/web/src/components/chat/RightPanelSurfaceContent.tsx"
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
    "https://github.com/toolboxmd/chromeria/issues/171",
    "https://github.com/toolboxmd/chromeria/issues/314"
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
    "apps/mobile/src/features/threads/useLinkedPullRequestActions.ts",
    "apps/server/src/environment/ServerEnvironment.ts",
    "apps/server/src/mcp/McpHttpServer.ts",
    "apps/server/src/mcp/toolkits/worktree/registration.test.ts",
    "apps/web/src/components/ChatView.tsx",
    "apps/web/src/components/RightPanelTabs.keyboard.browser.test.tsx",
    "apps/web/src/components/RightPanelTabs.tsx",
    "apps/web/src/components/pullRequest/ThreadPullRequestsPanel.tsx",
    "apps/web/src/reopenClosedView.test.ts",
    "apps/web/src/reopenClosedView.ts",
    "apps/web/src/rightPanelStore.test.ts",
    "apps/web/src/rightPanelStore.ts",
    "packages/client-runtime/src/pullRequestLinkInput.test.ts",
    "packages/client-runtime/src/pullRequestLinkInput.ts",
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
    "apps/web/src/components/files/BrowserDocumentFrame.tsx",
    "apps/web/src/components/chat/HtmlRenderDialog.browser.test.tsx",
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
    "packages/provider-core/src/server/subagentProjection.ts"
  ],
  "keywords": ["thread ownership", "coOwners", "person picker", "thread.share"]
}
```

## child-threads

```json
{
  "id": "child-threads",
  "purpose": "Create native children in any project, handle descendant requests, preserve durable subtree Stop and restart outcomes, offer parked delegated completions on explicit resume, retain upstream held-queue semantics until queue resume, and expose guarded agent queue resume.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/168",
    "https://github.com/toolboxmd/chromeria/issues/269",
    "https://github.com/toolboxmd/chromeria/issues/264",
    "https://github.com/toolboxmd/chromeria/issues/422",
    "https://github.com/toolboxmd/chromeria/issues/255"
  ],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/181",
    "https://github.com/toolboxmd/chromeria/pull/279",
    "https://github.com/toolboxmd/chromeria/pull/301",
    "https://github.com/toolboxmd/chromeria/pull/431",
    "https://github.com/toolboxmd/chromeria/pull/413"
  ],
  "newFiles": [
    "packages/client-runtime/src/state/restartCutPresentation.ts",
    "packages/client-runtime/src/state/restartCutPresentation.test.ts",
    "apps/server/src/childThreads/ForkCommitPlan.ts",
    "apps/server/src/childThreads/crossProject.test.ts",
    "apps/server/src/childThreads/delegatedIntake.ts",
    "apps/server/src/childThreads/delegatedFollowups.ts",
    "apps/server/src/childThreads/delegatedFollowupQueries.ts",
    "apps/server/src/childThreads/lineageBackfill.test.ts",
    "apps/server/src/childThreads/lineageBackfill.ts",
    "apps/server/src/childThreads/pendingRequests.test.ts",
    "apps/server/src/childThreads/pendingRequests.ts",
    "apps/server/src/childThreads/providerStop.test.ts",
    "apps/server/src/childThreads/requestWake.test.ts",
    "apps/server/src/childThreads/requestWake.startup.test.ts",
    "apps/server/src/childThreads/requestWake.ts",
    "apps/server/src/childThreads/retirement.test.ts",
    "apps/server/src/childThreads/retirement.ts",
    "apps/server/src/childThreads/stopDescendants.ts",
    "apps/server/src/childThreads/workspaceAccess.ts",
    "apps/server/src/orchestration-v2/DelegatedTaskQuestions.ts",
    "apps/server/src/orchestration-v2/ThreadManagementService.ancestorDelivery.test.ts",
    "apps/server/src/fork/ForkDispatchPlans.ts",
    "apps/server/src/fork/commitSequence.ts",
    "apps/server/src/fork/commitSequence.test.ts",
    "apps/server/src/mcp/QueueResume.integration.test.ts",
    "apps/server/src/orchestration-v2/RestartRecovery.integration.test.ts",
    "packages/shared/src/restartCut.ts",
    "apps/server/src/restartContinuation/RestartCut.test.ts",
    "apps/server/src/restartContinuation/EventSinkRestartCut.test.ts",
    "apps/server/src/restartContinuation/UnresolvedRestartCuts.test.ts",
    "apps/server/src/restartContinuation/RestartPersistence.test.ts",
    "apps/server/src/childThreads/PendingRequestIngress.ts",
    "apps/server/src/childThreads/PendingRequestService.ts",
    "apps/server/src/childThreads/pendingResponseAdmission.ts"
  ],
  "upstreamFiles": [
    "packages/client-runtime/src/state/threadDetail.ts",
    "apps/server/src/mcp/OrchestratorMcpService.activity.test.ts",
    "apps/server/src/mcp/OrchestratorMcpService.test.ts",
    "apps/server/src/mcp/OrchestratorMcpService.ts",
    "apps/server/src/mcp/OrchestratorMcpToolkit.integration.test.ts",
    "apps/server/src/mcp/threadAccess.ts",
    "apps/server/src/mcp/toolkits/attachment/tools.ts",
    "apps/server/src/mcp/toolkits/core.test.ts",
    "apps/server/src/mcp/toolkits/orchestrator/tools.test.ts",
    "apps/server/src/mcp/toolkits/thread/handlers.ts",
    "apps/server/src/mcp/toolkits/thread/tools.ts",
    "apps/server/src/orchestration-v2/EffectOutbox.ts",
    "apps/server/src/orchestration-v2/DelegatedTaskQuestionReactor.ts",
    "apps/server/src/orchestration-v2/EffectWorker.test.ts",
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
    "apps/server/src/orchestration-v2/ProviderContinuationService.ts",
    "apps/server/src/orchestration-v2/NotificationMailbox.ts",
    "apps/server/src/orchestration-v2/ProviderContinuationService.test.ts",
    "apps/server/src/orchestration-v2/testkit/ProviderReplayHarness.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "packages/contracts/src/orchestratorMcp.ts",
    "packages/provider-core/src/server/subagentProjection.ts",
    "apps/server/src/orchestration-v2/ProviderRuntimeRecoveryPerformance.test.ts",
    "apps/server/src/orchestration-v2/ProviderRuntimeRecoveryService.regression.test.ts",
    "apps/server/src/orchestration-v2/ProviderRuntimeRecoveryService.test.ts",
    "apps/server/src/orchestration-v2/RestartContinuation.test.ts",
    "apps/server/src/orchestration-v2/RestartContinuation.ts",
    "apps/server/src/orchestration-v2/SubagentProjection.test.ts",
    "packages/client-runtime/src/work-log/presentation.ts",
    "apps/server/src/orchestration-v2/Orchestrator.control-reads.test.ts"
  ],
  "sharedFiles": [
    "apps/server/src/persistence/forkV1Backfills.ts",
    "apps/server/src/mcp/toolkits/worktree/registration.test.ts",
    "apps/server/src/ws.ts",
    "apps/server/src/orchestration-v2/ProjectionStore.ts",
    "apps/server/src/orchestration-v2/ProviderRuntimeRecoveryService.ts",
    "apps/server/src/orchestration-v2/ProviderTurnControlService.test.ts",
    "apps/server/src/peer/RemoteDelegation.test.ts",
    "packages/client-runtime/src/t3ToolSummary.ts",
    "packages/shared/src/t3McpToolPresentation.ts"
  ],
  "keywords": [
    "delegate_task",
    "delegated_task.request",
    "forkRetirement",
    "forkResumedRetirements",
    "subagent_result",
    "lineageBackfill",
    "restartCut",
    "restartContinuationOfRunId",
    "continueRestartedRun",
    "t3_queue_resume"
  ]
}
```

## child-follow-up-wake

```json
{
  "id": "child-follow-up-wake",
  "purpose": "Deliver each finished requested follow-up once and rearm its parent wake while reusing the original delegated task and preserving its first result; exclude Monitor wakes. Upstream #17696 (c572cb71d2, already absorbed) fixes #13490 by creating fresh tasks, so it does not replace this adaptation. Retire when upstream preserves the same task identity and first result with equivalent per-run delivery, or the fork adopts its fresh-task lifecycle and the retained regressions pass.",
  "issues": [
    "https://github.com/pingdotgg/t3code/issues/13490",
    "https://github.com/toolboxmd/chromeria/issues/212",
    "https://github.com/toolboxmd/chromeria/issues/214"
  ],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/213",
    "https://github.com/toolboxmd/chromeria/pull/492"
  ],
  "newFiles": [],
  "upstreamFiles": [
    "apps/server/src/orchestration-v2/DelegatedCompletionDelivery.test.ts",
    "apps/server/src/orchestration-v2/ThreadDeletion.ts"
  ],
  "sharedFiles": [
    "apps/server/src/childThreads/delegatedFollowups.ts",
    "apps/server/src/childThreads/delegatedFollowupQueries.ts",
    "apps/server/src/mcp/OrchestratorMcpService.ts",
    "apps/server/src/mcp/OrchestratorMcpToolkit.integration.test.ts",
    "apps/server/src/mcp/delegatedTaskContinuation.test.ts",
    "packages/shared/src/forkToolInstructions.ts",
    "apps/server/src/mcp/toolkits/orchestrator/tools.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "apps/server/src/orchestration-v2/ProjectionStore.ts",
    "packages/provider-core/src/server/orchestrationInstructions.ts"
  ],
  "keywords": [
    "13490",
    "17696",
    "recordSubagentFollowup",
    "finalizeAppOwnedSubagent",
    "priorResultTransfers",
    "follow-up turn",
    "disposedBy"
  ]
}
```

## grok-background-command-end

```json
{
  "id": "grok-background-command-end",
  "purpose": "Finish Grok replies independently of background shells, track command ends on standalone and shared session methods, and keep command prose out of subagent identity (#435). Carries upstream #17144's command-end recognition plus fork Monitor wake reports, assertions and module-local fixture prompts. When #17144 merges, preserve these fork changes.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/435",
    "https://github.com/toolboxmd/chromeria/issues/215",
    "https://github.com/pingdotgg/t3code/issues/15888",
    "https://github.com/pingdotgg/t3code/pull/17144"
  ],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/216",
    "https://github.com/toolboxmd/chromeria/compare/main...fix/222-terminal-runs"
  ],
  "newFiles": [
    "apps/server/src/orchestration-v2/testkit/fixtures/grok_background_bash_killed/grok_transcript.ndjson",
    "apps/server/src/orchestration-v2/testkit/fixtures/grok_background_bash_killed/input.ts",
    "apps/server/src/orchestration-v2/testkit/fixtures/grok_background_bash_killed/output.ts",
    "apps/server/src/orchestration-v2/testkit/fixtures/grok_background_bash_polled/grok_transcript.ndjson",
    "apps/server/src/orchestration-v2/testkit/fixtures/grok_background_bash_polled/input.ts",
    "apps/server/src/orchestration-v2/testkit/fixtures/grok_background_bash_polled/output.ts",
    "apps/server/src/orchestration-v2/testkit/fixtures/grok_background_bash_unpolled/grok_transcript.ndjson",
    "apps/server/src/orchestration-v2/testkit/fixtures/grok_background_bash_unpolled/input.ts",
    "apps/server/src/orchestration-v2/testkit/fixtures/grok_background_bash_unpolled/output.ts"
  ],
  "upstreamFiles": [
    "packages/provider-grok/src/testing.ts",
    "apps/server/src/orchestration-v2/Adapters/AcpAdapterV2.test.ts",
    "packages/provider-acp/src/server/adapter.ts",
    "apps/server/src/orchestration-v2/testkit/fixtures/index.ts",
    "apps/server/src/orchestration-v2/testkit/fixtures/grok_background_bash/input.ts",
    "apps/server/src/orchestration-v2/testkit/fixtures/grok_background_bash/output.ts",
    "apps/server/src/orchestration-v2/testkit/fixtures/grok_background_bash_fast_wake/input.ts",
    "packages/provider-grok/src/server/xaiAcpExtension.test.ts",
    "packages/provider-grok/src/server/xaiAcpExtension.ts"
  ],
  "sharedFiles": [
    "packages/provider-grok/src/server/adapter.ts",
    "apps/server/src/orchestration-v2/testkit/fixtures/shared.ts"
  ],
  "keywords": [
    "17144",
    "15888",
    "435",
    "endReportedOutOfBand",
    "XAI_TASK_ID_RE",
    "KillTask",
    "isXAiNonBlockingBackgroundTool"
  ]
}
```

## grok-reasoning-effort

```json
{
  "id": "grok-reasoning-effort",
  "purpose": "Run Grok ACP threads at the selected reasoning effort. Carries upstream pingdotgg/t3code#15319 unchanged; drop this edit and take upstream's when #15319 merges.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/215",
    "https://github.com/pingdotgg/t3code/issues/15251",
    "https://github.com/pingdotgg/t3code/pull/15319"
  ],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/216"],
  "newFiles": [],
  "upstreamFiles": [
    "apps/server/src/orchestration-v2/Adapters/GrokAdapterV2.test.ts",
    "packages/provider-grok/src/server/adapter.ts"
  ],
  "sharedFiles": [
    "apps/server/src/orchestration-v2/Adapters/AcpAdapterV2.test.ts",
    "packages/provider-acp/src/server/adapter.ts"
  ],
  "keywords": ["15319", "15251", "reasoning_effort", "reasoningEffort"]
}
```

## prism-toolkit

```json
{
  "id": "prism-toolkit",
  "purpose": "Route upstream delegation and top-level launches through one Prism service and coordinate persisted same-provider/model recovery with immutable per-run outcomes, indexed admitted continuation sources and current recovery intent. Recover event changes only for their thread and parent; retain startup catch-up and the shared scheduler for unresolved recovery. Own Chromeria autoResumeLimitedThreads=true; Wight consumes it and saved false overrides remain respected.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/169",
    "https://github.com/toolboxmd/chromeria/issues/210",
    "https://github.com/toolboxmd/chromeria/issues/424",
    "https://github.com/toolboxmd/chromeria/issues/579"
  ],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/183",
    "https://github.com/toolboxmd/chromeria/pull/580"
  ],
  "newFiles": [
    "packages/provider-core/src/server/failure.network-recovery.test.ts",
    "packages/provider-cursor/src/server/adapter.network-recovery.test.ts",
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
    "apps/web/src/components/settings/SettingsPanels.restore.browser.test.tsx",
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
  "purpose": "Keep scoped Prism kits for the delegated roles (Planner, Researcher, Designer, Worker lanes, Computer use, Roast, Reviewer, Judge) and the stored Promachos kit on its own page editable with ordered Color lists and saved settings replay. Removed role kits drop at decode; stored records that name a removed role read as its successor. Project overrides are partial role maps over the environment kits; each page resets only its own kits.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/169",
    "https://github.com/toolboxmd/chromeria/issues/395",
    "https://github.com/toolboxmd/chromeria/issues/397",
    "https://github.com/toolboxmd/chromeria/issues/399",
    "https://github.com/toolboxmd/chromeria/issues/400"
  ],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/183",
    "https://github.com/toolboxmd/chromeria/pull/408",
    "https://github.com/toolboxmd/chromeria/pull/499"
  ],
  "newFiles": [
    "apps/server/src/prism/PrismRoleOverrides.test.ts",
    "apps/server/src/prism/PrismRoleOverrides.ts",
    "packages/shared/src/prismProjectOverrides.test.ts",
    "apps/web/src/components/settings/ColorList.tsx",
    "apps/web/src/components/settings/PromachosSettings.tsx",
    "apps/web/src/components/settings/PromachosSettings.logic.ts",
    "apps/web/src/components/settings/PromachosSettings.logic.test.ts",
    "apps/web/src/components/settings/usePrismSettingsSave.ts",
    "apps/web/src/routes/settings.promachos.tsx",
    "apps/web/src/components/settings/ColorPicker.logic.test.ts",
    "apps/web/src/components/settings/ColorPicker.logic.ts",
    "apps/web/src/components/settings/ColorPicker.tsx",
    "apps/web/src/components/settings/PrismSettings.list.test.tsx",
    "apps/web/src/components/settings/PrismSettings.logic.test.ts",
    "apps/web/src/components/settings/PrismSettings.logic.ts",
    "apps/web/src/components/settings/PrismSettings.state.test.ts",
    "apps/web/src/components/settings/PrismSettings.state.ts",
    "apps/web/src/components/settings/PrismSettings.tsx",
    "apps/web/src/routes/settings.prism.tsx"
  ],
  "upstreamFiles": [
    "apps/server/src/serverSettings.ts",
    "apps/web/src/components/settings/settingsSearch.test.ts",
    "apps/web/src/components/settings/SettingsSidebarNav.tsx",
    "apps/web/src/routeTree.gen.ts",
    "packages/shared/src/projectSettings.ts"
  ],
  "sharedFiles": [
    "apps/server/src/serverSettings.test.ts",
    "packages/client-runtime/src/state/server.ts",
    "packages/shared/src/serverSettings.ts",
    "apps/web/src/components/settings/settingsSearch.ts",
    "packages/contracts/src/settings.ts",
    "packages/contracts/src/prism.ts",
    "packages/contracts/src/prism.test.ts"
  ],
  "keywords": [
    "prismRoles",
    "PrismSettings",
    "worker lanes",
    "prismRole",
    "Researcher",
    "Roast",
    "Judge"
  ]
}
```

## run-groups

```json
{
  "id": "run-groups",
  "purpose": "Let one delegate_task call run several Colors of a read-only Prism role (Researcher, Designer, Roast, Reviewer, Judge) as one Run group: one command commits every member that can start, members share the caller's checkout, and the parent wakes once after every member is terminal. task_status and task_cancel take the groupId or a member; Run 1 keeps the single-task result.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/402",
    "https://github.com/toolboxmd/chromeria/issues/511"
  ],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/512"],
  "newFiles": [
    "apps/server/src/mcp/RunGroups.integration.test.ts",
    "apps/server/src/orchestration-v2/RunGroups.ts"
  ],
  "upstreamFiles": [],
  "sharedFiles": [
    "apps/server/src/mcp/OrchestratorMcpService.ts",
    "apps/server/src/mcp/toolkits/orchestrator/handlers.ts",
    "apps/server/src/mcp/toolkits/orchestrator/tools.ts",
    "apps/server/src/orchestration-v2/EffectOutbox.ts",
    "apps/server/src/orchestration-v2/EventSink.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "apps/server/src/orchestration-v2/ThreadManagementService.ts",
    "apps/server/src/prism/PrismService.ts",
    "apps/server/src/serverSettings.test.ts",
    "apps/web/src/components/settings/PrismSettings.logic.test.ts",
    "apps/web/src/components/settings/PrismSettings.logic.ts",
    "apps/web/src/components/settings/PrismSettings.tsx",
    "packages/contracts/src/orchestrationV2.ts",
    "packages/contracts/src/orchestratorMcp.ts",
    "packages/contracts/src/prism.test.ts",
    "packages/contracts/src/prism.ts",
    "packages/provider-core/src/server/orchestrationInstructions.ts"
  ],
  "keywords": ["Run group", "runGroupId", "delegated_task.group.request", "groupId"]
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
  "prs": ["https://github.com/toolboxmd/chromeria/pull/183"],
  "newFiles": [
    "apps/server/src/mcp/delegatedTaskContinuation.test.ts",
    "packages/shared/src/forkToolInstructions.ts",
    "apps/server/src/prism/ReplayInstructions.test.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/mcp/toolkits/preview/tools.test.ts",
    "packages/shared/package.json",
    "apps/server/src/mcp/toolkits/orchestrator/tools.ts",
    "packages/provider-core/src/server/runtimeInstructions.ts",
    "packages/provider-core/src/server/orchestrationInstructions.ts",
    "packages/provider-core/src/server/orchestrationInstructions.test.ts",
    "apps/server/src/orchestration-v2/testkit/ReplayRuntimeInstructions.ts"
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
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/172",
    "https://github.com/toolboxmd/chromeria/issues/269"
  ],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/184",
    "https://github.com/toolboxmd/chromeria/pull/279"
  ],
  "newFiles": [
    "apps/server/src/prism/staleTurnDetector.ts",
    "apps/server/src/prism/staleTurnDetector.test.ts",
    "apps/server/src/prism/staleTurnMonitor.ts",
    "apps/server/src/prism/staleTurnMonitor.integration.test.ts"
  ],
  "upstreamFiles": ["packages/client-runtime/src/state/threadWorkflows.test.ts"],
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
    "apps/server/src/promachos/PromachosClientCommands.test.ts",
    "apps/server/src/promachos/PromachosClientCommands.ts",
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
    "apps/mobile/src/lib/threadActivity.ts",
    "apps/server/src/auth/RpcAuthorization.test.ts",
    "apps/web/src/components/AppSidebarLayout.tsx",
    "apps/web/src/components/chat/ChatComposer.tsx",
    "apps/web/src/session-logic.ts",
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
    "apps/web/src/components/ScheduledCommandNotifications.browser.test.tsx",
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
    "apps/web/src/components/ThreadNotificationCoordinator.badge.browser.test.tsx",
    "apps/web/src/components/ThreadNotificationCoordinator.browser.test.tsx",
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

## contextmd-serving

Chromeria agent sessions enable Claude's native request recording and default to
whole requests. Explicit environment values win: `CLAUDE_CODE_ELEGANT_MEADOW=0`
turns recording off, and `CLAUDE_CODE_TETHER_LIVE=1` enables Message Threads;
either leaves context receipt unproven. Request variants remain in Claude's local
transcript. Receipt evidence in the serving log contains identities, times,
sources and reasons.

```json
{
  "id": "contextmd-serving",
  "purpose": "Serve ContextMD's stored rules and Great Library pointer to agents in Chromeria threads through thin harness hooks and native session boundaries, with serving state, settlement and the serving log in the environment.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/217",
    "https://github.com/toolboxmd/chromeria/issues/245",
    "https://github.com/toolboxmd/chromeria/issues/268",
    "https://github.com/toolboxmd/chromeria/issues/311",
    "https://github.com/toolboxmd/contextmd/issues/34"
  ],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/220",
    "https://github.com/toolboxmd/chromeria/pull/313",
    "https://github.com/toolboxmd/chromeria/pull/327"
  ],
  "newFiles": [
    "apps/server/src/contextmd/ContextmdRoute.test.ts",
    "apps/server/src/contextmd/ContextmdRoute.ts",
    "apps/server/src/contextmd/ContextmdServing.ts",
    "apps/server/src/contextmd/ContextmdServing.test.ts",
    "apps/server/src/contextmd/Harnesses.ts",
    "apps/server/src/contextmd/Harnesses.test.ts",
    "apps/server/src/contextmd/Library.test.ts",
    "apps/server/src/contextmd/Matcher.ts",
    "apps/server/src/contextmd/Reads.ts",
    "apps/server/src/contextmd/Select.test.ts",
    "apps/server/src/contextmd/Select.ts",
    "apps/server/src/contextmd/Selection.ts",
    "apps/server/src/contextmd/ServingState.test.ts",
    "apps/server/src/contextmd/ServingState.ts",
    "apps/server/src/contextmd/Shown.test.ts",
    "apps/server/src/contextmd/Shown.ts",
    "apps/server/src/contextmd/Store.ts",
    "apps/server/src/contextmd/library-corpus.json",
    "apps/server/src/contextmd/GreatLibrary.test.ts",
    "apps/server/src/contextmd/great-library-path-corpus.json"
  ],
  "upstreamFiles": [
    "apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.test.ts",
    "apps/server/src/orchestration-v2/CheckpointRestoreSafety.test.ts",
    "apps/server/src/orchestration-v2/CheckpointRollbackService.test.ts",
    "apps/server/src/orchestration-v2/CheckpointRollbackService.ts",
    "packages/provider-core/src/server/ProviderAdapter.ts",
    "apps/server/src/orchestration-v2/ProviderTurnStartService.test.ts",
    "apps/server/src/orchestration-v2/ProviderTurnStartService.ts",
    "apps/server/src/orchestration-v2/testkit/ProviderTurnStartMemory.fixture.mjs",
    "apps/server/src/provider/acp/AcpJsonRpcConnection.test.ts",
    "packages/provider-acp/src/server/AcpSessionRuntime.ts"
  ],
  "sharedFiles": [
    "apps/server/src/server.ts",
    "apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.ts",
    "apps/server/src/orchestration-v2/Adapters/AcpAdapterV2.test.ts",
    "packages/provider-acp/src/server/adapter.ts",
    "apps/server/src/orchestration-v2/ProviderSessionManager.test.ts",
    "apps/server/src/orchestration-v2/ProviderSessionManager.ts",
    "apps/server/src/orchestration-v2/runtimeLayer.ts",
    "apps/server/src/orchestration-v2/testkit/ProviderReplayHarness.ts"
  ],
  "keywords": [
    "contextmd",
    "ContextMD",
    "/contextmd/v1",
    "contextmd.sqlite",
    "contextmd-hook",
    "great_library"
  ]
}
```

## single-run-status

```json
{
  "id": "single-run-status",
  "purpose": "Read only the current run for provider turn-start and finalization status guards, preserving recovery reads for inherited background work. Drop when upstream uses equivalent filtered status reads.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/426"],
  "prs": ["https://github.com/toolboxmd/chromeria/compare/main...perf/426-single-run-status"],
  "newFiles": [],
  "upstreamFiles": [],
  "sharedFiles": [
    "apps/server/src/orchestration-v2/ProviderTurnStartService.bare.test.ts",
    "apps/server/src/orchestration-v2/ProviderTurnStartService.test.ts",
    "apps/server/src/orchestration-v2/ProviderTurnStartService.ts",
    "apps/server/src/orchestration-v2/testkit/ProviderTurnStartMemory.fixture.mjs"
  ],
  "keywords": ["isCurrentAttemptInStatus", "shouldFinalizeRun", "runIds"]
}
```

## bounded-stored-event-replay

```json
{
  "id": "bounded-stored-event-replay",
  "purpose": "Replay stored orchestration events from an explicit cursor through the paged event-store read, so a missing cursor cannot decode the whole log.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/256"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/267"],
  "newFiles": [],
  "upstreamFiles": [
    "apps/server/src/orchestration-v2/testkit/ProviderSwitch.integration.test.ts",
    "apps/server/src/relay/AgentAwarenessRelay.test.ts"
  ],
  "sharedFiles": [
    "apps/server/src/orchestration-v2/EventSink.ts",
    "apps/server/src/orchestration-v2/FoundationPersistence.test.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "apps/server/src/orchestration-v2/ThreadLaunchService.test.ts",
    "apps/server/src/orchestration-v2/ThreadManagementService.ts",
    "apps/server/src/orchestration-v2/testkit/OrchestratorScenario.ts"
  ],
  "keywords": ["afterSequence", "catchUp", "streamStoredEvents"]
}
```

## feature-map

```json
{
  "id": "feature-map",
  "purpose": "Draw a project's docs/map.md in a right-panel Map tab, each part coloured planned, working, broken or done from GitHub, with live base parts, scoped Chromeria marks, working-thread icons and a gaps list.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/235",
    "https://github.com/toolboxmd/chromeria/issues/374"
  ],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/247",
    "https://github.com/toolboxmd/chromeria/compare/feat/374-holistic-map...feat/374-holistic-map-live"
  ],
  "newFiles": [
    "apps/server/src/issues/featureMap.test.ts",
    "apps/server/src/issues/featureMap.ts",
    "apps/web/src/components/featureMap/FeatureMapPanel.tsx",
    "apps/web/src/components/featureMap/featureMap.logic.test.ts",
    "apps/web/src/components/featureMap/featureMap.logic.ts",
    "apps/web/src/components/featureMap/featureMap.mermaid.test.tsx",
    "apps/web/src/state/featureMap.ts",
    "packages/contracts/src/featureMap.test.ts",
    "packages/contracts/src/featureMap.ts"
  ],
  "upstreamFiles": ["apps/web/src/components/chat/MermaidDiagram.tsx"],
  "sharedFiles": [
    "apps/server/src/auth/RpcAuthorization.ts",
    "apps/server/src/environment/ServerEnvironment.ts",
    "apps/server/src/issues/IssueService.test.ts",
    "apps/server/src/issues/IssueService.ts",
    "apps/server/src/issues/issueRpcHandlers.ts",
    "apps/web/src/components/ChatView.tsx",
    "apps/web/src/components/RightPanelTabs.tsx",
    "apps/web/src/components/issues/issueStatus.logic.ts",
    "apps/web/src/components/issues/useIssueRowThreads.ts",
    "apps/web/src/rightPanelStore.test.ts",
    "apps/web/src/rightPanelStore.ts",
    "docs/user/source-control.md",
    "packages/contracts/src/environment.ts",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/issues.test.ts",
    "packages/contracts/src/rpc.ts"
  ],
  "keywords": ["featureMap", "feature map", "docs/map.md", "primary ref", "context part"]
}
```

## macos-temp-realpath-tests

```json
{
  "id": "macos-temp-realpath-tests",
  "purpose": "Install ownership tests compare the resolved temp root, because the server reports the real npm prefix and macOS temp dirs sit behind the /var symlink. Drop when upstream resolves the same paths in these tests.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/246"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/262"],
  "newFiles": [],
  "upstreamFiles": [
    "apps/server/src/cli/invocation.test.ts",
    "apps/server/src/environment/ServerEnvironment.test.ts"
  ],
  "sharedFiles": [],
  "keywords": ["realPath", "/private/var", "npm-global"]
}
```

## secret-store-atomic-create

```json
{
  "id": "secret-store-atomic-create",
  "purpose": "ServerSecretStore.create writes the complete secret to a temporary file and hard-links it into place, so a creator that loses the race never reads a half-written secret. Drop when upstream makes create atomic.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/249"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/261"],
  "newFiles": [],
  "upstreamFiles": [
    "apps/server/src/auth/ServerSecretStore.test.ts",
    "apps/server/src/auth/ServerSecretStore.ts"
  ],
  "sharedFiles": [],
  "keywords": ["getOrCreateRandom", "fileSystem.link", "half-written secret"]
}
```

## peer-links

```json
{
  "id": "peer-links",
  "purpose": "Port upstream server peer links and record-first remote delegation until absorption replaces the port; retain permanent pairing-code sessions, target Prism launch intent and recovery-aware waits. Extend routing to all MCP toolkit groups while retaining caller capabilities and link-mode narrowing. Own explicit peer consent, including its mobile settings entry and destination permission checks. Replay delegated lifecycle results durably and open qualified destination work through the source client connection. Bind negotiated requests to verified durable caller identity, transfer attachment bytes through destination staging, reconcile native acceptance and retain truthful unknown outcomes for unreconcilable effects. Surface qualified descendant questions and approvals through verified mixed ancestry and the authenticated source client, with native revision guards and receipt replay. Keep source viewers and question choices readable across clients, with exact transport diagnostics behind a disclosure. Align sparse remote follow-up selection with full projections so initial work appears once. Execute typed device commands on their exact native session with current originating authority, pre-open authenticated command discovery and qualified native artifact output. Retain real remote-parent provenance in client shells, hide destination delegated children from default navigation and keep them in scoped existing search even when the parent is offline. Apply default-child hiding to phone Home, Promachos conversations and legacy rendered/jump lists while keeping existing search on full qualified shells. Open source-owned browser and device viewers through current resource-bound relays, acquire verified qualified inputs and results, and preserve exact session lifecycle across web, desktop and mobile. Keep accepted destination schedules and Spectrum rounds local within immutable contributor bounds, and recover exact disable acknowledgements after ordinary link retirement.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/164",
    "https://github.com/toolboxmd/chromeria/issues/302",
    "https://github.com/toolboxmd/chromeria/issues/367",
    "https://github.com/toolboxmd/chromeria/issues/380",
    "https://github.com/toolboxmd/chromeria/issues/381",
    "https://github.com/toolboxmd/chromeria/issues/382",
    "https://github.com/toolboxmd/chromeria/issues/369",
    "https://github.com/toolboxmd/chromeria/issues/383",
    "https://github.com/toolboxmd/chromeria/issues/379",
    "https://github.com/toolboxmd/chromeria/issues/386",
    "https://github.com/toolboxmd/chromeria/issues/384"
  ],
  "prs": [
    "https://github.com/pingdotgg/t3code/pull/16653",
    "https://github.com/pingdotgg/t3code/pull/16654",
    "https://github.com/pingdotgg/t3code/pull/16655",
    "https://github.com/pingdotgg/t3code/pull/16684",
    "https://github.com/pingdotgg/t3code/pull/16695",
    "https://github.com/pingdotgg/t3code/pull/16719",
    "https://github.com/toolboxmd/chromeria/pull/304",
    "https://github.com/toolboxmd/chromeria/pull/368",
    "https://github.com/toolboxmd/chromeria/pull/391",
    "https://github.com/toolboxmd/chromeria/pull/410",
    "https://github.com/toolboxmd/chromeria/pull/416",
    "https://github.com/toolboxmd/chromeria/pull/420",
    "https://github.com/toolboxmd/chromeria/pull/421",
    "https://github.com/toolboxmd/chromeria/pull/539",
    "https://github.com/toolboxmd/chromeria/pull/546",
    "https://github.com/toolboxmd/chromeria/pull/534",
    "https://github.com/toolboxmd/chromeria/pull/555"
  ],
  "newFiles": [
    "apps/server/src/scheduledTasks/DestinationAutomationAuthority.ts",
    "apps/server/src/scheduledTasks/DestinationAutomationAuthority.test.ts",
    "packages/contracts/src/peerTask.ts",
    "packages/client-runtime/src/state/remoteTasks.ts",
    "packages/client-runtime/src/state/remoteTasks.test.ts",
    "apps/web/src/state/remoteTasks.ts",
    "apps/web/src/components/chat/RemoteTaskViewer.tsx",
    "apps/web/src/components/chat/useSubagentOpen.tsx",
    "apps/web/src/components/chat/BackgroundSubagentOpenButton.tsx",
    "apps/mobile/src/state/remote-tasks.ts",
    "apps/mobile/src/features/threads/RemoteTaskViewer.tsx",
    "apps/server/src/mcp/delegatedTaskModes.ts",
    "apps/server/src/mcp/linkOrigin.test.ts",
    "apps/server/src/mcp/linkOrigin.ts",
    "apps/server/src/orchestration-v2/RemoteDelegatedTask.test.ts",
    "apps/server/src/peer/*",
    "apps/web/src/components/settings/PeerGrantSettings.tsx",
    "apps/server/src/persistence/forkPeerLinksSchema.test.ts",
    "apps/server/src/persistence/forkPeerLinksSchema.ts",
    "packages/contracts/src/peerLink.ts",
    "apps/server/src/mcp/McpRouting.ts",
    "apps/server/src/persistence/forkScheduledTaskOriginsSchema.ts",
    "apps/server/src/persistence/forkScheduledTaskOriginsSchema.test.ts",
    "apps/web/src/components/chat/V2LifecycleRow.remote.browser.test.tsx",
    "apps/web/src/state/entities.remoteRelationships.test.ts",
    "apps/web/src/components/chat/V2LifecycleRow.remote.test.tsx",
    "apps/server/src/assets/AttachmentStaging.test.ts",
    "apps/server/src/assets/AttachmentStaging.ts",
    "apps/server/src/mcp/McpRequestReceipts.test.ts",
    "apps/server/src/mcp/McpRequestReceipts.ts",
    "apps/server/src/mcp/McpRequestRuntime.ts",
    "apps/server/src/mcp/McpToolEffects.ts",
    "apps/server/src/mcp/requestIdentity.ts",
    "apps/server/src/scheduledTasks/mutationReceipts.ts",
    "packages/contracts/src/mcpRequest.ts",
    "packages/contracts/src/peerAttachments.ts",
    "packages/contracts/src/peerAuthority.ts",
    "apps/mobile/src/features/threads/RemotePendingRequest.test.tsx",
    "apps/mobile/src/features/threads/RemotePendingRequest.tsx",
    "apps/web/src/components/chat/RemotePendingRequest.browser.test.tsx",
    "apps/web/src/components/chat/RemotePendingRequest.tsx",
    "apps/server/src/device/AgentDeviceCommand.ts",
    "apps/server/src/device/AgentDeviceCommand.test.ts",
    "apps/server/src/device/DeviceCommands.registered.test.ts",
    "apps/server/src/device/testing/LegacyDeviceTools.fixture.ts",
    "apps/server/src/device/testing/LegacyDeviceHandlers.fixture.ts",
    "apps/mobile/src/features/home/HomeRouteScreen.search.test.tsx",
    "apps/web/src/components/LegacySidebar.test.ts",
    "apps/mobile/src/features/browser/preview-stream-document.test.ts",
    "apps/mobile/src/features/browser/BrowserPreviewRouteScreen.test.tsx",
    "apps/mobile/src/features/browser/PreviewStreamWebView.test.tsx",
    "apps/server/src/assets/NativeBrowserDownload.test.ts",
    "apps/server/src/http/ResourceWebSocketServer.test.ts",
    "apps/server/src/http/ResourceWebSocketServer.ts",
    "apps/server/src/preview/testing/BrowserExternal.fixture.ts",
    "packages/client-runtime/src/state/deviceHubAccess.test.ts"
  ],
  "upstreamFiles": [
    "packages/client-runtime/src/device/screenshot.ts",
    "packages/client-runtime/src/device/screenshot.test.ts",
    "apps/web/src/state/entities.ts",
    "packages/client-runtime/src/state/subagentRuntime.ts",
    "apps/mobile/src/features/settings/SettingsEnvironmentDetailRouteScreen.tsx",
    "packages/client-runtime/src/state/commandPermissions.test.ts",
    "apps/server/src/auth/McpOAuth.ts",
    "apps/server/src/binCli.ts",
    "apps/server/src/cli/environment.ts",
    "apps/server/src/mcp/McpHttpServer.test.ts",
    "apps/server/src/mcp/McpInvocationContext.ts",
    "apps/server/src/mcp/McpToolAccess.test.ts",
    "apps/server/src/mcp/McpToolAccess.ts",
    "apps/server/src/mcp/TaskCancelNativeSubagent.integration.test.ts",
    "apps/server/src/mcp/WorktreeMcpService.ts",
    "apps/server/src/mcp/toolkits/attachment/handlers.ts",
    "apps/server/src/mcp/toolkits/orchestrator/handlers.ts",
    "apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.ts",
    "apps/server/src/orchestration-v2/ProviderRuntimeRecoveryService.ts",
    "apps/server/src/orchestration-v2/ProviderTurnControlService.test.ts",
    "apps/server/src/orchestration-v2/ThreadManagementService.test.ts",
    "apps/server/src/orchestration-v2/runtimeLayer.test.ts",
    "apps/server/src/project/ProjectEnrichmentService.ts",
    "apps/server/src/project/ProjectService.ts",
    "apps/server/src/project/ProjectSetupScriptRunner.test.ts",
    "apps/server/src/project/ProjectSetupScriptRunner.ts",
    "packages/client-runtime/src/t3ToolSummary.ts",
    "packages/shared/src/t3McpToolPresentation.ts",
    "apps/server/src/htmlRender/HtmlRender.ts",
    "packages/shared/src/t3McpToolPresentation.test.ts",
    "apps/server/src/htmlRender/HtmlRender.test.ts",
    "apps/server/src/mcp/toolkits/html/handlers.ts",
    "apps/server/src/mcp/McpSessionRegistry.test.ts",
    "apps/server/src/mcp/McpSessionRegistry.testkit.ts",
    "apps/server/src/mcp/McpSessionRegistry.ts",
    "apps/server/src/mcp/toolkits/device/tools.ts",
    "apps/server/src/mcp/toolkits/environment/tools.ts",
    "apps/server/src/mcp/toolkits/html/tools.ts",
    "apps/server/src/mcp/toolkits/preview/tools.ts",
    "apps/server/src/mcp/toolkits/previewControls/tools.ts",
    "apps/server/src/mcp/toolkits/pullRequests/tools.ts",
    "apps/server/src/mcp/toolkits/worktree/tools.ts",
    "apps/server/src/orchestration-v2/OpenCode2OrchestratorV2.live.test.ts",
    "docs/internals/remote.md",
    "packages/contracts/src/assets.ts",
    "apps/mobile/src/features/threads/SubagentRow.tsx",
    "apps/mobile/src/features/threads/ThreadAgentsSheet.tsx",
    "apps/mobile/src/features/threads/thread-subagent-group.tsx",
    "apps/mobile/src/features/threads/threadAgentsPresentation.test.ts",
    "apps/mobile/src/features/threads/threadAgentsPresentation.ts",
    "apps/web/src/components/chat/ThreadRelationshipsControl.agents.browser.test.tsx",
    "apps/web/src/components/chat/ThreadRelationshipsControl.tsx",
    "apps/web/src/components/chat/V2ItemInspector.tsx",
    "apps/web/src/components/chat/V2LifecycleRow.tsx",
    "packages/client-runtime/src/state/threadExecution.test.ts",
    "packages/client-runtime/src/state/threadExecution.ts",
    "packages/client-runtime/src/state/threadRelationships.test.ts",
    "packages/client-runtime/src/state/presentation.test.ts",
    "packages/client-runtime/src/state/threadRelationships.ts",
    "apps/mobile/src/features/threads/thread-work-log.tsx",
    "packages/client-runtime/src/work-log/itemDetail.test.ts",
    "packages/client-runtime/src/work-log/itemDetail.ts",
    "packages/shared/src/orchestrationV2PendingBackgroundWork.test.ts",
    "packages/shared/src/orchestrationV2PendingBackgroundWork.ts",
    "apps/server/src/assets/AttachmentUpload.ts",
    "apps/server/src/mcp/ThreadMetadataMcpService.ts",
    "apps/server/src/mcp/toolkits/attachment/input.ts",
    "apps/server/src/mcp/toolkits/device/handlers.ts",
    "apps/server/src/mcp/toolkits/environment/handlers.ts",
    "apps/server/src/mcp/toolkits/preview/handlers.test.ts",
    "apps/server/src/mcp/toolkits/previewControls/handlers.ts",
    "apps/server/src/mcp/toolkits/pullRequests/handlers.test.ts",
    "apps/server/src/mcp/toolkits/pullRequests/handlers.ts",
    "apps/server/src/mcp/toolkits/worktree/handlers.ts",
    "apps/server/src/orchestration-v2/AttachmentClaims.ts",
    "apps/server/src/project/ManagedProjectFolders.test.ts",
    "apps/server/src/project/ManagedProjectFolders.ts",
    "apps/mobile/src/features/threads/ThreadDetailScreen.tsx",
    "packages/client-runtime/src/state/models.ts",
    "packages/client-runtime/src/state/threadShell.test.ts",
    "packages/client-runtime/src/state/threadShell.ts",
    "packages/shared/src/agentAwareness.test.ts",
    "packages/shared/src/agentAwareness.ts",
    "apps/server/src/orchestration-v2/WireProjection.test.ts",
    "apps/server/src/orchestration-v2/ProjectionStore.test.ts",
    "packages/client-runtime/src/t3ToolSummary.test.ts",
    "apps/server/src/device/DeviceToolchain.ts",
    "apps/server/src/mcp/McpDeviceToolkit.test.ts",
    "apps/mobile/src/features/home/HomeRouteScreen.tsx",
    "apps/web/src/components/LegacySidebar.tsx",
    "apps/mobile/src/features/browser/BrowserPreviewRouteScreen.tsx",
    "apps/mobile/src/features/browser/PreviewStreamWebView.tsx",
    "apps/mobile/src/features/browser/preview-stream-document.ts",
    "apps/mobile/src/features/browser/preview-stream.browser.ts",
    "apps/mobile/src/features/devices/DevicePreviewRouteScreen.tsx",
    "apps/mobile/src/features/devices/threadDevicePreviews.test.ts",
    "apps/mobile/src/features/devices/threadDevicePreviews.ts",
    "apps/mobile/src/state/device.ts",
    "apps/mobile/src/state/preview.ts",
    "apps/server/src/assets/AssetAccess.ts",
    "apps/server/src/device/DeviceHubProxy.test.ts",
    "apps/server/src/device/DeviceHubProxy.ts",
    "apps/server/src/device/DeviceService.test.ts",
    "apps/server/src/http.ts",
    "apps/server/src/preview/Manager.ts",
    "apps/server/src/preview/ServerBrowserPage.test.ts",
    "apps/server/src/preview/ServerBrowserStream.ts",
    "apps/server/src/preview/SessionControl.test.ts",
    "apps/server/src/preview/SessionControl.ts",
    "apps/web/src/browser/ServerBrowserSurface.tsx",
    "apps/web/src/browser/serverPictureInPicture.ts",
    "apps/web/src/components/device/DeviceStreamView.browser.test.tsx",
    "apps/web/src/components/device/DeviceStreamView.tsx",
    "apps/web/src/components/preview/PreviewPanel.tsx",
    "apps/web/src/components/preview/ThreadPreviewMiniPlayer.tsx",
    "apps/web/src/previewMiniPlayerStore.test.ts",
    "apps/web/src/previewMiniPlayerStore.ts",
    "apps/web/src/state/device.ts",
    "apps/web/src/state/previewStream.ts",
    "packages/client-runtime/src/device/hubAccess.ts",
    "packages/client-runtime/src/device/stream.test.ts",
    "packages/client-runtime/src/device/stream.ts",
    "packages/client-runtime/src/preview/serverBrowserStream.test.ts",
    "packages/client-runtime/src/preview/serverBrowserStream.ts",
    "packages/client-runtime/src/state/assets.test.ts",
    "packages/client-runtime/src/state/assets.ts",
    "packages/client-runtime/src/state/device.ts",
    "packages/client-runtime/src/state/deviceHubAccess.ts",
    "packages/client-runtime/src/state/preview.ts",
    "packages/contracts/src/assets.test.ts",
    "packages/contracts/src/preview.test.ts",
    "packages/contracts/src/preview.ts",
    "packages/shared/src/toolOutput.test.ts",
    "packages/shared/src/toolOutput.ts"
  ],
  "sharedFiles": [
    "vite.config.ts",
    "apps/server/src/preview/ServerBrowserStream.test.ts",
    "apps/server/src/preview/ServerBrowser.ts",
    "apps/server/src/forkWsRpc.ts",
    "apps/server/src/orchestration-v2/EventSink.ts",
    "apps/server/src/persistence/AuthSessions.ts",
    "apps/server/src/scheduledTaskChecks/store.ts",
    "apps/server/src/spectrum/LaunchService.ts",
    "apps/server/src/spectrum/RoundService.ts",
    "apps/server/src/spectrum/barrier.test.ts",
    "apps/server/src/spectrum/commandPlan.ts",
    "apps/server/src/spectrum/state.ts",
    "apps/server/src/spectrum/store.ts",
    "packages/client-runtime/src/state/threadDetail.ts",
    "apps/server/src/device/DeviceService.ts",
    "apps/web/src/components/device/DevicePanel.tsx",
    "packages/contracts/src/device.ts",
    "apps/mobile/src/features/threads/ThreadDetailScreen.tsx",
    "packages/client-runtime/src/state/models.ts",
    "apps/server/src/orchestration-v2/EffectWorker.ts",
    "packages/client-runtime/package.json",
    "apps/server/src/childThreads/delegatedFollowups.ts",
    "apps/server/src/childThreads/delegatedFollowupQueries.ts",
    "apps/server/src/orchestration-v2/DelegatedTaskQuestions.ts",
    "packages/contracts/src/clientRpcPermissions.ts",
    "docs/user/remote-access.md",
    "apps/server/src/mcp/toolkits/orchestrator/tools.test.ts",
    "apps/server/src/auth/EnvironmentAuth.ts",
    "apps/server/src/auth/RpcAuthorization.ts",
    "apps/server/src/auth/SessionStore.ts",
    "apps/server/src/auth/SessionStore.test.ts",
    "apps/server/src/environment/ServerEnvironment.ts",
    "apps/server/src/mcp/McpHttpServer.ts",
    "apps/server/src/mcp/OrchestratorMcpService.activity.test.ts",
    "apps/server/src/mcp/OrchestratorMcpService.test.ts",
    "apps/server/src/mcp/OrchestratorMcpService.ts",
    "apps/server/src/mcp/OrchestratorMcpToolkit.integration.test.ts",
    "apps/server/src/mcp/threadAccess.ts",
    "apps/server/src/mcp/toolkits/core.test.ts",
    "apps/server/src/mcp/toolkits/orchestrator/tools.ts",
    "apps/server/src/mcp/toolkits/project/handlers.test.ts",
    "apps/server/src/mcp/toolkits/project/handlers.ts",
    "apps/server/src/mcp/toolkits/project/tools.ts",
    "apps/server/src/mcp/toolkits/worktree/registration.test.ts",
    "apps/server/src/observability/RpcInstrumentation.ts",
    "apps/server/src/orchestration-v2/DelegatedCompletionDelivery.test.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "apps/server/src/orchestration-v2/ProjectionStore.ts",
    "apps/server/src/orchestration-v2/ThreadLaunchService.test.ts",
    "apps/server/src/orchestration-v2/ThreadLaunchService.ts",
    "apps/server/src/orchestration-v2/ThreadManagementService.ts",
    "apps/server/src/orchestration-v2/runtimeLayer.ts",
    "apps/server/src/prism/RecoveryCoordinator.ts",
    "apps/server/src/prism/RecoveryHistory.ts",
    "packages/provider-core/src/server/orchestrationInstructions.ts",
    "apps/server/src/relay/AgentAwarenessRelay.test.ts",
    "apps/server/src/server.ts",
    "apps/server/src/spectrum/mcpScopes.test.ts",
    "apps/server/src/spectrum/mcpToolkit.ts",
    "apps/server/src/ws.ts",
    "packages/contracts/src/auth.ts",
    "packages/contracts/src/environment.ts",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "packages/contracts/src/orchestratorMcp.ts",
    "packages/contracts/src/rpc.ts",
    "scripts/fork-features.mjs",
    "apps/server/src/persistence/Sqlite.ts",
    "apps/server/src/scheduledTasks/ScheduledTaskService.ts",
    "packages/contracts/src/scheduledTask.ts",
    "apps/server/src/scheduledTaskChecks/state.ts",
    "apps/server/src/scheduledTaskChecks/engine.ts",
    "apps/server/src/scheduledTaskChecks/engine.test.ts",
    "apps/server/src/scheduledTaskChecks/ScheduledTaskChecks.ts",
    "apps/server/src/issueLinks/IssueLinks.ts",
    "apps/server/src/mcp/toolkits/attachment/tools.ts",
    "apps/server/src/mcp/toolkits/thread/handlers.ts",
    "apps/server/src/mcp/toolkits/thread/tools.ts",
    "apps/server/src/orchestration-v2/ThreadMessageIntake.ts",
    "apps/server/src/scheduledTaskChecks/DispatchPolicy.ts",
    "apps/server/src/orchestration-v2/DelegatedTaskQuestionReactor.ts",
    "apps/web/src/components/ChatView.tsx",
    "docs/user/thread-sidebar.md",
    "apps/mobile/src/features/threads/ThreadNavigationSidebar.tsx",
    "apps/mobile/src/features/threads/threadListV2.test.ts",
    "apps/web/src/components/Sidebar.logic.ts",
    "apps/web/src/components/Sidebar.logic.test.ts",
    "apps/web/src/components/Sidebar.tsx",
    "apps/mobile/src/features/home/HomeScreen.tsx",
    "apps/web/src/components/promachos/promachosConversations.ts",
    "apps/web/src/components/promachos/promachosConversations.test.ts",
    "apps/server/src/auth/RpcAuthorization.test.ts",
    "apps/web/src/components/chat/MessagesTimeline.tsx",
    "apps/web/src/rightPanelStore.test.ts",
    "apps/web/src/rightPanelStore.ts",
    "packages/contracts/src/environmentHttp.ts",
    "apps/server/src/mcp/PreviewAutomationBroker.ts",
    "apps/server/src/mcp/toolkits/preview/handlers.ts",
    "apps/server/src/preview/ServerBrowserPage.ts",
    "packages/contracts/src/previewAutomation.ts"
  ],
  "keywords": [
    "peer_environment_links",
    "t3code-peer-link",
    "remoteChild",
    "RemoteDelegation",
    "RecoveryWait",
    "T3-Mode-Limit",
    "delegatedFrom"
  ]
}
```

## nested-cli-startup

```json
{
  "id": "nested-cli-startup",
  "purpose": "Carry upstream's connected-IPC gate so t3 in agent shells ignores an ancestor's launcher context. Drop this port when upstream PR #14263 is absorbed.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/295"],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/303",
    "https://github.com/pingdotgg/t3code/pull/14263"
  ],
  "newFiles": [],
  "upstreamFiles": [
    "apps/server/src/cloud/serviceLauncherClient.test.ts",
    "apps/server/src/cloud/serviceLauncherClient.ts"
  ],
  "sharedFiles": [],
  "keywords": ["T3_SERVICE_LAUNCHER_CONTEXT", "resolveStartup", "ipc-unavailable"]
}
```

## paired-session-renewal

```json
{
  "id": "paired-session-renewal",
  "purpose": "Keep paired desktop, web and mobile sessions active through rotating device-held renewal authority, with headless approval for existing devices.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/323",
    "https://github.com/toolboxmd/chromeria/issues/330",
    "https://github.com/toolboxmd/chromeria/issues/325"
  ],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/334",
    "https://github.com/toolboxmd/chromeria/pull/504",
    "https://github.com/toolboxmd/chromeria/pull/490"
  ],
  "newFiles": [
    "apps/desktop/src/app/DesktopRenewalStore.test.ts",
    "apps/desktop/src/app/DesktopRenewalStore.ts",
    "apps/mobile/src/connection/renewalStorage.ts",
    "apps/server/src/auth/BrowserRenewalCommitments.test.ts",
    "apps/server/src/auth/BrowserRenewalCommitments.ts",
    "apps/server/src/auth/forkCrossProcessRevocation.test.ts",
    "apps/server/src/auth/forkCrossProcessRevocation.ts",
    "apps/server/src/auth/SessionRenewal.test.ts",
    "apps/server/src/auth/SessionRenewal.ts",
    "apps/web/src/connection/renewalStorage.ts",
    "packages/client-runtime/src/authorization/renewal.test.ts",
    "packages/client-runtime/src/authorization/renewal.ts",
    "packages/client-runtime/src/connection/renewalRecordStore.ts",
    "packages/shared/src/sessionRenewal.ts"
  ],
  "upstreamFiles": [
    "apps/desktop/src/ipc/DesktopIpcHandlers.ts",
    "apps/desktop/src/ipc/channels.ts",
    "apps/desktop/src/ipc/methods/connectionCatalog.ts",
    "apps/desktop/src/main.ts",
    "apps/desktop/src/preload.ts",
    "apps/mobile/src/lib/runtime.ts",
    "apps/server/src/auth/http.test.ts",
    "apps/server/src/cli/auth.ts",
    "apps/server/src/cli/browser.ts",
    "apps/web/src/authBootstrap.test.ts",
    "apps/web/src/lib/runtime.test.ts",
    "apps/web/src/lib/runtime.ts",
    "apps/web/src/routes/__root.tsx",
    "apps/web/src/routes/_chat.tsx",
    "apps/web/src/routes/pair.tsx",
    "docs/user/remote-access.md",
    "packages/client-runtime/src/authorization/index.ts",
    "packages/client-runtime/src/authorization/remote.ts",
    "packages/client-runtime/src/connection/index.ts",
    "packages/client-runtime/src/connection/onboarding.ts",
    "packages/client-runtime/src/connection/resolver.ts",
    "packages/client-runtime/src/connection/supervisor.test.ts",
    "packages/client-runtime/src/connection/supervisor.ts",
    "packages/contracts/src/ipc.ts"
  ],
  "sharedFiles": [
    "apps/server/src/auth/EnvironmentAuth.ts",
    "apps/server/src/auth/SessionStore.test.ts",
    "apps/server/src/auth/SessionStore.ts",
    "apps/server/src/auth/http.ts",
    "apps/web/src/environments/primary/auth.ts",
    "apps/web/test/environmentHttpTest.ts",
    "packages/contracts/src/auth.ts",
    "packages/contracts/src/environmentHttp.ts",
    "packages/shared/package.json"
  ],
  "keywords": [
    "SessionRenewal",
    "renewal_commitment",
    "renewal-commitments",
    "awaitInvalidation",
    "data_version"
  ]
}
```

## bare-launch

```json
{
  "id": "bare-launch",
  "purpose": "Strip removable harness context, admit only named additions within caller permissions, and retain durable intent and honest native receipts across launch and recovery paths.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/289"],
  "prs": ["https://github.com/toolboxmd/chromeria/compare/main...feat/289-bare-launch"],
  "newFiles": [
    "apps/server/src/orchestration-v2/BareLaunch.recovery.integration.test.ts",
    "apps/server/src/orchestration-v2/Adapters/BareAdapters.test.ts",
    "apps/server/src/orchestration-v2/BareLaunch.test.ts",
    "apps/server/src/orchestration-v2/BareLaunch.ts",
    "apps/server/src/orchestration-v2/ProviderTurnStartService.bare.test.ts",
    "apps/server/src/orchestration-v2/bareReadGrant.ts",
    "apps/server/src/peer/PeerForwarding.bare.test.ts",
    "apps/server/src/persistence/forkScheduledTaskBareSchema.ts",
    "apps/server/src/scheduledTasks/ScheduledTaskService.bare.test.ts",
    "apps/server/src/scheduledTasks/bareState.ts",
    "packages/contracts/src/bareLaunch.test.ts",
    "packages/contracts/src/bareLaunch.ts",
    "packages/provider-core/src/server/BareProviderRuntime.ts",
    "packages/provider-core/src/server/bareLaunch.ts",
    "packages/provider-grok/src/server/GrokBareRuntime.test.ts",
    "packages/provider-grok/src/server/GrokBareRuntime.ts"
  ],
  "upstreamFiles": [
    "apps/server/package.json",
    "apps/server/src/orchestration-v2/AcpRegistryOrchestratorV2.live.test.ts",
    "apps/server/src/orchestration-v2/Adapters/CodexAdapterV2.test.ts",
    "apps/server/src/orchestration-v2/Adapters/CodexAdapterV2.ts",
    "apps/server/src/orchestration-v2/ContextHandoffService.test.ts",
    "apps/server/src/orchestration-v2/ContextHandoffService.ts",
    "apps/server/src/orchestration-v2/CursorOrchestratorV2.live.test.ts",
    "apps/server/src/orchestration-v2/GrokOrchestratorV2.live.test.ts",
    "apps/server/src/orchestration-v2/ProjectSettingsUpgrade.integration.test.ts",
    "apps/server/src/orchestration-v2/ProviderSwitchService.ts",
    "apps/server/src/orchestration-v2/RuntimePolicy.test.ts",
    "apps/server/src/orchestration-v2/RuntimePolicy.ts",
    "apps/server/src/orchestration-v2/ThreadFork.execution.test.ts",
    "apps/server/src/scheduledTasks/ScheduledTaskService.schedule.test.ts",
    "apps/server/src/scheduledTasks/ScheduledTaskService.test.ts",
    "apps/server/src/scheduledTasks/ScheduledTaskService.webhook.test.ts",
    "apps/server/src/scheduling/Scheduler.integration.test.ts",
    "docs/user/outside-agents.md",
    "packages/provider-core/package.json",
    "packages/provider-core/src/server/failure.ts",
    "packages/provider-cursor/src/server/adapter.ts",
    "packages/provider-cursor/src/server/sdk.ts",
    "packages/provider-grok/package.json",
    "packages/provider-grok/src/server/acpSupport.ts",
    "packages/provider-opencode/src/server/OpenCodeRuntime.ts",
    "packages/provider-opencode/src/server/adapter.test.ts",
    "packages/provider-opencode/src/server/adapter.ts",
    "packages/provider-opencode/src/server/driver.ts"
  ],
  "sharedFiles": [
    "GLOSSARY.md",
    "apps/server/src/mcp/OrchestratorMcpService.activity.test.ts",
    "apps/server/src/mcp/OrchestratorMcpService.test.ts",
    "apps/server/src/mcp/OrchestratorMcpService.ts",
    "apps/server/src/mcp/OrchestratorMcpToolkit.integration.test.ts",
    "apps/server/src/mcp/TaskCancelNativeSubagent.integration.test.ts",
    "apps/server/src/mcp/delegatedTaskModes.ts",
    "apps/server/src/mcp/linkOrigin.test.ts",
    "apps/server/src/mcp/toolkits/core.test.ts",
    "apps/server/src/mcp/toolkits/project/handlers.test.ts",
    "apps/server/src/mcp/toolkits/project/handlers.ts",
    "apps/server/src/mcp/toolkits/project/tools.ts",
    "apps/server/src/mcp/toolkits/thread/handlers.ts",
    "apps/server/src/mcp/toolkits/thread/tools.ts",
    "apps/server/src/mcp/toolkits/worktree/registration.test.ts",
    "apps/server/src/orchestration-v2/Adapters/AcpAdapterV2.test.ts",
    "apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.test.ts",
    "apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.ts",
    "apps/server/src/orchestration-v2/Adapters/GrokAdapterV2.test.ts",
    "apps/server/src/orchestration-v2/CheckpointRollbackService.test.ts",
    "apps/server/src/orchestration-v2/CheckpointRollbackService.ts",
    "apps/server/src/orchestration-v2/ClaudeAutomaticDelivery.integration.test.ts",
    "apps/server/src/orchestration-v2/DelegatedCompletionDelivery.test.ts",
    "apps/server/src/orchestration-v2/OpenCode2OrchestratorV2.live.test.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "apps/server/src/orchestration-v2/ProviderSessionManager.test.ts",
    "apps/server/src/orchestration-v2/ProviderSessionManager.ts",
    "apps/server/src/orchestration-v2/ProviderTurnStartService.test.ts",
    "apps/server/src/orchestration-v2/ProviderTurnStartService.ts",
    "apps/server/src/orchestration-v2/RestartContinuation.ts",
    "apps/server/src/orchestration-v2/RestartRecovery.integration.test.ts",
    "apps/server/src/orchestration-v2/RunExecutionService.ts",
    "apps/server/src/orchestration-v2/ThreadLaunchService.test.ts",
    "apps/server/src/orchestration-v2/ThreadLaunchService.ts",
    "apps/server/src/orchestration-v2/ThreadMessageIntake.test.ts",
    "apps/server/src/orchestration-v2/ThreadMessageIntake.ts",
    "apps/server/src/orchestration-v2/runtimeLayer.test.ts",
    "apps/server/src/orchestration-v2/runtimeLayer.ts",
    "apps/server/src/orchestration-v2/testkit/ProviderReplayHarness.ts",
    "apps/server/src/orchestration-v2/testkit/ProviderTurnStartMemory.fixture.mjs",
    "apps/server/src/peer/PeerForwarding.test.ts",
    "apps/server/src/peer/PeerForwarding.ts",
    "apps/server/src/peer/RemoteDelegation.test.ts",
    "apps/server/src/peer/RemoteDelegation.ts",
    "apps/server/src/persistence/Sqlite.ts",
    "apps/server/src/prism/PrismService.test.ts",
    "apps/server/src/prism/PrismService.ts",
    "apps/server/src/promachos/PromachosLaunch.ts",
    "apps/server/src/provider/acp/AcpJsonRpcConnection.test.ts",
    "apps/server/src/scheduledTaskChecks/ScheduledTaskChecks.integration.test.ts",
    "apps/server/src/scheduledTaskChecks/ScheduledTaskChecks.ts",
    "apps/server/src/scheduledTaskChecks/engine.test.ts",
    "apps/server/src/scheduledTaskChecks/engine.ts",
    "apps/server/src/scheduledTaskChecks/state.ts",
    "apps/server/src/scheduledTasks/ScheduledTaskService.ts",
    "apps/server/src/server.ts",
    "apps/server/src/spectrum/Controller.integration.test.ts",
    "apps/server/src/spectrum/Launch.integration.test.ts",
    "apps/server/src/spectrum/LaunchService.ts",
    "apps/server/src/spectrum/ReportService.integration.test.ts",
    "apps/server/src/spectrum/controllerTestkit.ts",
    "apps/server/src/spectrum/humanStopRpc.integration.test.ts",
    "apps/server/src/spectrum/mcpScopes.test.ts",
    "apps/server/src/spectrum/mcpSend.integration.test.ts",
    "apps/server/src/spectrum/mcpToolkit.ts",
    "apps/server/src/ws.ts",
    "docs/map.md",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "packages/contracts/src/orchestratorMcp.ts",
    "packages/contracts/src/scheduledTask.ts",
    "packages/provider-acp/src/server/AcpSessionRuntime.ts",
    "packages/provider-acp/src/server/adapter.ts",
    "packages/provider-core/src/server/ProviderAdapter.ts",
    "packages/provider-grok/src/server/adapter.ts",
    "pnpm-lock.yaml"
  ],
  "keywords": [
    "BareLaunch",
    "bareReceipt",
    "bareReadGrant",
    "include_instructions",
    "systemPromptOverride"
  ]
}
```

## no-project-sidebar

```json
{
  "id": "no-project-sidebar",
  "purpose": "Keep temporary projectless threads in a collapsed sidebar section across web, desktop and native mobile.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/356"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/357"],
  "newFiles": [],
  "upstreamFiles": [
    "apps/mobile/src/features/home/HomeScreen.tsx",
    "apps/mobile/src/features/home/useThreadListActions.ts",
    "apps/mobile/src/features/home/useThreadListActions.test.ts",
    "apps/mobile/src/features/threads/ThreadArrangementSheet.tsx",
    "apps/mobile/src/features/threads/ThreadNavigationSidebar.tsx",
    "apps/mobile/src/features/threads/thread-list-v2-items.tsx",
    "apps/mobile/src/features/threads/threadListV2.test.ts",
    "apps/mobile/src/features/threads/threadListV2.ts",
    "apps/mobile/src/features/threads/threadOrder.ts",
    "apps/mobile/src/features/threads/use-thread-list-v2-shelf-preferences.ts",
    "apps/mobile/src/lib/storage.test.ts",
    "apps/mobile/src/persistence/mobile-preferences.ts",
    "apps/mobile/src/state/thread-order.ts",
    "apps/mobile/src/state/thread-order.test.ts",
    "apps/web/src/components/Sidebar.drag.test.ts",
    "apps/web/src/components/Sidebar.drag.ts",
    "apps/web/src/components/Sidebar.logic.test.ts",
    "apps/web/src/components/Sidebar.logic.ts"
  ],
  "sharedFiles": ["apps/web/src/components/Sidebar.tsx"],
  "keywords": ["noProjectShelfExpanded", "no-project-header", "No project"]
}
```

## rejected-connect-link

```json
{
  "id": "rejected-connect-link",
  "purpose": "Preserve environment-credential tunnel recovery after service shutdown on top of native rejected-link state and explicit CLI relink.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/358",
    "https://github.com/toolboxmd/chromeria/issues/362"
  ],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/359"],
  "newFiles": [],
  "upstreamFiles": [
    "apps/server/src/cloud/CloudLink.lifecycle.test.ts",
    "apps/server/src/cloud/CloudLink.ts"
  ],
  "sharedFiles": ["packages/contracts/src/environmentHttp.ts"],
  "keywords": ["cloud-link-revoked", "dropRevokedCloudLink", "linkRevoked"]
}
```

## network-routes

```json
{
  "id": "network-routes",
  "purpose": "Learn usable T3 Connect fallback routes from the signed-in account while preserving direct routes and sign-out cleanup.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/292",
    "https://github.com/toolboxmd/chromeria/issues/363",
    "https://github.com/toolboxmd/chromeria/issues/361"
  ],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/364",
    "https://github.com/toolboxmd/chromeria/pull/498",
    "https://github.com/toolboxmd/chromeria/pull/497"
  ],
  "newFiles": [
    "packages/client-runtime/src/relay/eligibility.ts",
    "packages/client-runtime/src/relay/eligibility.test.ts"
  ],
  "upstreamFiles": [
    "apps/mobile/src/features/cloud/managedRelayState.ts",
    "apps/mobile/src/features/connection/environmentSections.test.ts",
    "apps/mobile/src/features/connection/environmentSections.ts",
    "apps/mobile/src/features/connection/useConnectionController.ts",
    "apps/web/src/cloud/managedRelayState.ts",
    "apps/web/src/components/cloud/CloudEnvironmentConnectList.tsx",
    "packages/client-runtime/src/relay/index.ts",
    "packages/client-runtime/src/relay/managedRelayState.test.ts",
    "packages/client-runtime/src/relay/managedRelayState.ts",
    "apps/web/src/components/cloud/CloudEnvironmentConnectList.browser.test.tsx",
    "packages/client-runtime/src/connection/layer.ts",
    "packages/client-runtime/src/connection/onboarding.test.ts",
    "packages/client-runtime/src/connection/registry.test.ts",
    "packages/client-runtime/src/connection/registry.ts",
    "packages/client-runtime/src/relay/discovery.test.ts",
    "packages/client-runtime/src/relay/discovery.ts",
    "packages/client-runtime/src/state/threads-atoms.test.ts"
  ],
  "sharedFiles": [
    "docs/user/remote-access.md",
    "apps/web/src/components/settings/ConnectionsSettings.tsx"
  ],
  "keywords": [
    "isAddableRelayEnvironment",
    "listedSession",
    "reconcileRelayRoutes",
    "watchAccountRelayRoutes"
  ]
}
```

## unchanged-turn-items

```json
{
  "id": "unchanged-turn-items",
  "purpose": "Ingest shared-session turn-item deliveries once across overlapping run observers, and skip unchanged snapshots at the event sink; carried upstream fix.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/425"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/433"],
  "newFiles": ["apps/server/src/orchestration-v2/testkit/TurnItemRedelivery.integration.test.ts"],
  "upstreamFiles": [],
  "sharedFiles": [
    "apps/server/src/fork/commitSequence.ts",
    "apps/server/src/orchestration-v2/EventSink.ts",
    "apps/server/src/orchestration-v2/FoundationPersistence.test.ts",
    "apps/server/src/orchestration-v2/ProjectionStore.ts",
    "apps/server/src/orchestration-v2/ProviderSessionManager.ts",
    "apps/server/src/orchestration-v2/ProviderSessionManager.test.ts",
    "apps/server/src/orchestration-v2/RunExecutionService.ts"
  ],
  "keywords": ["pendingItems", "itemIngestions", "encodeTurnItemPayload"]
}
```

## parent-run-observers

```json
{
  "id": "parent-run-observers",
  "purpose": "Leave app-owned child event ingestion with its own run so later parent roots release their observers at terminal; preserve native background draining and durable parent wakes.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/456"],
  "prs": ["https://github.com/toolboxmd/chromeria/compare/main...fix/456-parent-run-observers"],
  "newFiles": [],
  "upstreamFiles": [],
  "sharedFiles": [
    "apps/server/src/orchestration-v2/ProviderSessionManager.test.ts",
    "apps/server/src/orchestration-v2/ProviderTurnStartService.ts",
    "apps/server/src/orchestration-v2/RunExecutionService.ts",
    "apps/server/src/orchestration-v2/RunExecutionService.test.ts"
  ],
  "keywords": ["canRouteRelatedSubagent", "relatedThreadIds", "activeChildProviderTurns"]
}
```

## agent-web-research

```json
{
  "id": "agent-web-research",
  "purpose": "Read research pages through one caller-gated MCP service with full text, typed failures, isolated headless rendering for thin/consent pages, a shared page cache, an origin limiter and provenance; optionally share one linked environment's reader with caller limits and cache ownership preserved; use a pinned zero-charge TinyFish fallback with shared quotas and bounded, nonpersistent hosted continuation; fan out locale/site discovery through SearXNG and pinned TinyFish search with per-engine health, shared spacing and budgets, and no stored SERPs. Explicit default-off session reads reuse an existing profile tab on its owner, with caller/session-private continuation and no enforced pacing.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/423",
    "https://github.com/toolboxmd/chromeria/issues/440",
    "https://github.com/toolboxmd/chromeria/issues/442",
    "https://github.com/toolboxmd/chromeria/issues/443",
    "https://github.com/toolboxmd/chromeria/issues/444",
    "https://github.com/toolboxmd/chromeria/issues/445",
    "https://github.com/toolboxmd/chromeria/issues/446"
  ],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/449",
    "https://github.com/toolboxmd/chromeria/pull/453",
    "https://github.com/toolboxmd/chromeria/pull/455",
    "https://github.com/toolboxmd/chromeria/pull/502",
    "https://github.com/toolboxmd/chromeria/pull/505",
    "https://github.com/toolboxmd/chromeria/pull/513"
  ],
  "newFiles": [
    "apps/server/src/webResearch/Search.ts",
    "apps/server/src/webResearch/SessionRead.ts",
    "apps/server/src/mcp/WebResearchSession.integration.test.ts",
    "apps/server/src/webResearch/WebSearchHost.test.ts",
    "apps/server/src/mcp/WebSearch.integration.test.ts",
    "packages/contracts/src/webResearch.ts",
    "apps/server/src/mcp/WebResearch.integration.test.ts",
    "apps/server/src/mcp/WebResearchRender.integration.test.ts",
    "apps/server/src/mcp/toolkits/webResearch/handlers.test.ts",
    "apps/server/src/mcp/toolkits/webResearch/handlers.ts",
    "apps/server/src/mcp/toolkits/webResearch/tools.ts",
    "apps/server/src/webResearch/PageCache.ts",
    "apps/server/src/webResearch/PageCache.test.ts",
    "apps/server/src/webResearch/TinyFish.ts",
    "apps/server/src/webResearch/TinyFish.test.ts",
    "apps/server/src/mcp/WebResearchTinyFish.integration.test.ts",
    "apps/server/src/webResearch/PublicHttp.test.ts",
    "apps/server/src/webResearch/PublicHttp.ts",
    "apps/server/src/webResearch/PublicRender.test.ts",
    "apps/server/src/webResearch/PublicRender.ts",
    "apps/server/src/webResearch/ResearchHost.test.ts",
    "apps/server/src/webResearch/ResearchHost.ts",
    "apps/server/src/webResearch/WebResearch.ts",
    "apps/server/src/webResearch/extract.ts",
    "apps/server/src/webResearch/schemas.ts",
    "packages/contracts/src/researchHost.test.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/htmlRender/publicProxy.ts",
    "apps/server/src/preview/ServerBrowserContexts.ts",
    "packages/contracts/src/previewAutomation.ts"
  ],
  "sharedFiles": [
    "apps/server/src/preview/ServerBrowser.test.ts",
    "apps/server/src/preview/ServerBrowser.ts",
    "GLOSSARY.md",
    "apps/server/package.json",
    "apps/server/src/mcp/McpHttpServer.ts",
    "apps/server/src/mcp/McpRouting.ts",
    "apps/server/src/mcp/McpToolAccess.ts",
    "apps/server/src/mcp/toolkits/worktree/registration.test.ts",
    "apps/server/src/auth/RpcAuthorization.test.ts",
    "apps/server/src/serverSettings.test.ts",
    "docs/map.md",
    "docs/user/remote-access.md",
    "packages/contracts/src/settings.ts",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/orchestratorMcp.ts",
    "packages/provider-core/src/server/orchestrationInstructions.ts",
    "pnpm-lock.yaml"
  ],
  "keywords": [
    "web_read",
    "web_search",
    "WebResearch",
    "ResearchHost",
    "web-cache",
    "ChromeriaResearch"
  ]
}
```

## login-shell-capture

```json
{
  "id": "login-shell-capture",
  "purpose": "Keep installed harnesses available when the desktop login-shell capture is slow or fails, and let the server try again only when that capture did not return a PATH.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/458"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/459"],
  "newFiles": [],
  "upstreamFiles": [
    "apps/desktop/src/shell/DesktopShellEnvironment.test.ts",
    "apps/desktop/src/shell/DesktopShellEnvironment.ts",
    "apps/server/src/os-jank.test.ts",
    "packages/shared/src/shell.test.ts",
    "packages/shared/src/shell.ts"
  ],
  "sharedFiles": [],
  "keywords": ["LOGIN_SHELL_CAPTURE_TIMEOUT_MS", "shellEnvironmentPrepared", "login-shell"]
}
```

## legacy-terminal-labels

```json
{
  "id": "legacy-terminal-labels",
  "purpose": "Preserve embedded legacy terminal labels after supplementary Unicode letters across Node runtimes.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/345"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/412"],
  "newFiles": [],
  "upstreamFiles": [
    "packages/shared/src/composerContextLegacy.test.ts",
    "packages/shared/src/composerContextLegacy.ts"
  ],
  "sharedFiles": [],
  "keywords": ["upgradeLegacyContextMessage", "supplementary Unicode", "terminal labels"]
}
```

## launcher-test-fixtures

```json
{
  "id": "launcher-test-fixtures",
  "purpose": "Stop launcher test fixtures on IPC owner loss and await captured child exits during teardown. Drop when upstream provides equivalent fixture lifetime handling and regression coverage.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/157"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/414"],
  "newFiles": [],
  "upstreamFiles": ["apps/server/src/serviceLauncher.test.ts"],
  "sharedFiles": [],
  "keywords": ["launcher fixture ownership", "spawnedChildren", "serviceLauncher.test.ts"]
}
```

## menu-escape-focus

```json
{
  "id": "menu-escape-focus",
  "purpose": "Preserve Escape focus return when the pointer rests over a closing menu. Drop the BaseUI patch and exact version pin after upgrading to a release with equivalent late-hover-close protection, such as mui/base-ui#4893, and proving pointer-over Escape plus hover, submenu and outside-focus controls pass without the patch. Retain regression coverage until upstream provides equivalent tests.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/576"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/578"],
  "newFiles": [
    "apps/web/src/components/ui/menu.focus.browser.test.tsx",
    "patches/@base-ui__react@1.5.0.patch"
  ],
  "upstreamFiles": ["apps/web/package.json"],
  "sharedFiles": ["pnpm-lock.yaml", "pnpm-workspace.yaml"],
  "keywords": ["onFloatingMouseLeave", "@base-ui/react", "menu-escape-focus"]
}
```

## preview-chrome-identity

```json
{
  "id": "preview-chrome-identity",
  "purpose": "Apply a paired Chrome identity to listed preview main-frame navigations without holding their requests, reconcile aborts with the committed page and restore after debugger reattachment.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/460",
    "https://github.com/toolboxmd/chromeria/issues/524"
  ],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/470"],
  "newFiles": [
    "apps/desktop/src/preview/ChromeUserAgent.ts",
    "apps/desktop/src/preview/ChromeUserAgent.test.ts",
    "apps/desktop/src/preview/ChromeIdentitySession.ts",
    "apps/desktop/src/preview/ChromeIdentitySession.test.ts"
  ],
  "upstreamFiles": [
    "apps/desktop/src/preview/Manager.ts",
    "apps/desktop/src/preview/Manager.test.ts"
  ],
  "sharedFiles": [],
  "keywords": ["preview user agent", "ChromeUserAgent", "ChromeIdentitySession"]
}
```

## execution-purpose

```json
{
  "id": "execution-purpose",
  "purpose": "Pin typed Issue or PR purpose at run admission and retain supported native causal evidence without treating job binding as outcome ownership.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/377"],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/388",
    "https://github.com/toolboxmd/chromeria/pull/581"
  ],
  "newFiles": [
    "apps/mobile/src/features/threads/ExecutionPurposePicker.tsx",
    "apps/mobile/src/features/threads/ThreadExecutionPurposeControl.tsx",
    "apps/server/src/orchestration-v2/ExecutionPurpose.integration.test.ts",
    "apps/server/src/orchestration-v2/ExecutionPurpose.ts",
    "apps/server/src/persistence/forkExecutionPurposeSchema.ts",
    "apps/web/src/components/chat/ExecutionPurposeControl.tsx",
    "packages/contracts/src/executionPurpose.test.ts",
    "packages/contracts/src/executionPurpose.ts",
    "packages/contracts/src/nativeExecution.ts"
  ],
  "upstreamFiles": [
    "apps/mobile/src/features/threads/McpAppWebView.tsx",
    "apps/mobile/src/features/threads/NewTaskDraftScreen.tsx",
    "apps/mobile/src/state/pending-thread-creation.ts",
    "apps/mobile/src/state/thread-outbox-model.ts",
    "apps/mobile/src/state/thread-outbox.test.ts",
    "apps/mobile/src/state/use-thread-composer-state.ts",
    "apps/mobile/src/state/use-thread-outbox-drain.ts",
    "packages/contracts/src/threadPullRequest.ts",
    "packages/provider-core/src/server/ProviderContinuationRequests.ts",
    "packages/provider-muse/src/server/adapter.ts",
    "packages/provider-pi/src/server/adapter.ts"
  ],
  "sharedFiles": [
    "apps/server/src/mcp/toolkits/pullRequests/handlers.test.ts",
    "apps/server/src/mcp/toolkits/pullRequests/handlers.ts",
    "apps/mobile/src/features/threads/ThreadDetailScreen.tsx",
    "packages/client-runtime/src/state/models.ts",
    "GLOSSARY.md",
    "apps/mobile/src/features/settings/SettingsScheduledTasksRouteScreen.tsx",
    "apps/mobile/src/features/settings/scheduledTaskDraft.ts",
    "apps/mobile/src/features/threads/ThreadRouteScreen.tsx",
    "apps/mobile/src/state/use-thread-selection.ts",
    "apps/server/src/childThreads/requestWake.ts",
    "apps/server/src/environment/ServerEnvironment.ts",
    "apps/server/src/mcp/OrchestratorMcpService.ts",
    "apps/server/src/mcp/OrchestratorMcpToolkit.integration.test.ts",
    "apps/server/src/mcp/RunGroups.integration.test.ts",
    "apps/server/src/mcp/toolkits/core.test.ts",
    "apps/server/src/mcp/toolkits/project/handlers.ts",
    "apps/server/src/mcp/toolkits/project/tools.ts",
    "apps/server/src/mcp/toolkits/pullRequests/tools.ts",
    "apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.test.ts",
    "apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.ts",
    "apps/server/src/orchestration-v2/Adapters/CodexAdapterV2.test.ts",
    "apps/server/src/orchestration-v2/Adapters/CodexAdapterV2.ts",
    "apps/server/src/orchestration-v2/ClaudeAutomaticDelivery.integration.test.ts",
    "apps/server/src/orchestration-v2/CommandPolicy.ts",
    "apps/server/src/orchestration-v2/DelegatedCompletionDelivery.test.ts",
    "apps/server/src/orchestration-v2/DelegatedTaskQuestions.ts",
    "apps/server/src/orchestration-v2/EffectWorker.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "apps/server/src/orchestration-v2/ProjectionStore.ts",
    "apps/server/src/orchestration-v2/ProviderContinuationService.test.ts",
    "apps/server/src/orchestration-v2/ProviderContinuationService.ts",
    "apps/server/src/orchestration-v2/ProviderTurnControlService.ts",
    "apps/server/src/orchestration-v2/RunExecutionService.ts",
    "apps/server/src/orchestration-v2/SteeringCompletion.integration.test.ts",
    "apps/server/src/orchestration-v2/ThreadLaunchService.test.ts",
    "apps/server/src/orchestration-v2/ThreadLaunchService.ts",
    "apps/server/src/orchestration-v2/ThreadManagementService.ts",
    "apps/server/src/orchestration-v2/ThreadStop.test.ts",
    "apps/server/src/orchestration-v2/runtimeLayer.test.ts",
    "apps/server/src/peer/PeerForwarding.test.ts",
    "apps/server/src/peer/PeerForwarding.ts",
    "apps/server/src/peer/RemoteDelegation.test.ts",
    "apps/server/src/peer/RemoteDelegation.ts",
    "apps/server/src/persistence/Sqlite.ts",
    "apps/server/src/prism/staleTurnMonitor.ts",
    "apps/server/src/scheduledTaskChecks/ScheduledTaskChecks.integration.test.ts",
    "apps/server/src/scheduledTaskChecks/ScheduledTaskChecks.ts",
    "apps/server/src/scheduledTaskChecks/engine.test.ts",
    "apps/server/src/scheduledTaskChecks/engine.ts",
    "apps/server/src/scheduledTaskChecks/state.ts",
    "apps/server/src/scheduledTasks/ScheduledTaskService.bare.test.ts",
    "apps/server/src/scheduledTasks/ScheduledTaskService.schedule.test.ts",
    "apps/server/src/scheduledTasks/ScheduledTaskService.ts",
    "apps/server/src/scheduledTasks/ScheduledTaskService.webhook.test.ts",
    "apps/server/src/scheduling/Scheduler.integration.test.ts",
    "apps/server/src/ws.ts",
    "apps/web/src/components/ChatView.tsx",
    "apps/web/src/components/pullRequest/ThreadPullRequestsPanel.tsx",
    "apps/web/src/components/settings/ScheduledTasksSettings.tsx",
    "apps/web/src/components/settings/scheduledTasksSettings.logic.ts",
    "docs/map.md",
    "docs/user/thread-sidebar.md",
    "packages/client-runtime/src/operations/commands.test.ts",
    "packages/client-runtime/src/operations/commands.ts",
    "packages/contracts/src/environment.ts",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "packages/contracts/src/orchestratorMcp.ts",
    "packages/contracts/src/scheduledTask.ts",
    "packages/provider-acp/src/server/adapter.ts",
    "packages/provider-core/src/server/ProviderAdapter.ts",
    "packages/provider-cursor/src/server/adapter.ts",
    "packages/provider-opencode/src/server/adapter.test.ts",
    "packages/provider-opencode/src/server/adapter.ts",
    "packages/provider-opencode/src/server/v2/adapter.ts"
  ],
  "keywords": ["purposeIntent", "nativeSegments", "executionPurposeVersions"]
}
```

## chrome-login-mirror

```json
{
  "id": "chrome-login-mirror",
  "purpose": "Continuously mirror macOS Chrome cookies into persistent desktop partitions and authenticated environment browser profiles, preserving source-only removals and complete partition keys.",
  "issues": [
    "https://github.com/toolboxmd/chromeria/issues/520",
    "https://github.com/toolboxmd/chromeria/issues/541"
  ],
  "prs": [
    "https://github.com/toolboxmd/chromeria/pull/530",
    "https://github.com/toolboxmd/chromeria/pull/545"
  ],
  "newFiles": [
    "apps/desktop/src/preview/BrowserImport/ChromiumKeychainWorker.ts",
    "apps/desktop/src/preview/BrowserImport/ChromiumKeychainWorker.test.ts",
    "apps/desktop/src/preview/BrowserImport/ChromiumKeychain.test.ts",
    "apps/desktop/src/preview/DesktopCookieMirror.startup.test.ts",
    "apps/desktop/src/ipc/methods/cookieMirror.ts",
    "apps/desktop/src/preview/ChromeCookieKey.test.ts",
    "apps/desktop/src/preview/ChromeCookieKey.ts",
    "apps/desktop/src/preview/ChromeCookieSource.test.ts",
    "apps/desktop/src/preview/ChromeCookieSource.ts",
    "apps/desktop/src/preview/CookieMirror.test.ts",
    "apps/desktop/src/preview/CookieMirror.ts",
    "apps/desktop/src/preview/CookieMirrorDestination.test.ts",
    "apps/desktop/src/preview/CookieMirrorDestination.ts",
    "apps/desktop/src/preview/CookieMirrorDpop.test.ts",
    "apps/desktop/src/preview/CookieMirrorDpop.ts",
    "apps/desktop/src/preview/CookieMirrorGrant.test.ts",
    "apps/desktop/src/preview/CookieMirrorGrant.ts",
    "apps/desktop/src/preview/CookieMirrorMetadata.test.ts",
    "apps/desktop/src/preview/CookieMirrorMetadata.ts",
    "apps/desktop/src/preview/CookieMirrorSocket.test.ts",
    "apps/desktop/src/preview/CookieMirrorSocket.ts",
    "apps/desktop/src/preview/DesktopCookieMirror.test.ts",
    "apps/desktop/src/preview/DesktopCookieMirror.ts",
    "apps/desktop/src/preview/ElectronCookieMirror.integration.test.ts",
    "apps/desktop/src/preview/ElectronCookieMirror.ts",
    "apps/desktop/src/preview/PartitionScope.ts",
    "apps/server/src/preview/BrowserCookieMirror.test.ts",
    "apps/server/src/preview/BrowserCookieMirror.ts",
    "apps/server/src/preview/CookieMirrorRoute.test.ts",
    "apps/server/src/preview/CookieMirrorRoute.ts",
    "apps/server/src/preview/ServerCookieMirror.integration.test.ts",
    "apps/server/src/preview/ServerCookieMirror.ts",
    "apps/server/src/preview/ServerCookieMirrorProfiles.ts",
    "apps/web/src/components/settings/ChromeCookieMirrorStatus.tsx",
    "apps/web/src/rpc/cookieMirrorConnection.ts",
    "apps/web/src/rpc/cookieMirrorStatus.ts",
    "packages/contracts/src/browserCookieMirror.test.ts",
    "packages/contracts/src/browserCookieMirror.ts",
    "packages/shared/src/browserCookieMirror.test.ts",
    "packages/shared/src/browserCookieMirror.ts"
  ],
  "upstreamFiles": [
    "apps/desktop/scripts/main-process-bundle.test.mjs",
    "apps/desktop/src/preview/BrowserImport/ChromiumKeys.module.test.ts",
    "apps/desktop/src/preview/BrowserImport/ChromiumKeys.test.ts",
    "apps/desktop/vite.config.ts",
    "apps/desktop/src/backend/DesktopLocalEnvironmentAuth.ts",
    "apps/desktop/src/ipc/methods/preview.test.ts",
    "apps/desktop/src/ipc/methods/preview.ts",
    "apps/desktop/src/preview/BrowserImport/BrowserImport.ts",
    "apps/desktop/src/preview/BrowserImport/ChromiumKeys.ts",
    "apps/desktop/src/settings/DesktopAppSettings.test.ts",
    "apps/desktop/src/settings/DesktopAppSettings.ts",
    "apps/server/src/auth/PairingGrantStore.ts",
    "apps/server/src/preview/ServerBrowser.test.ts",
    "apps/server/src/preview/ServerBrowser.ts",
    "apps/server/src/preview/ServerBrowserStream.test.ts",
    "apps/web/src/components/settings/IntegrationsSettings.browser.test.tsx",
    "apps/web/src/connection/runtime.ts",
    "apps/web/src/hooks/useSettings.test.ts",
    "apps/web/src/hooks/useSettings.ts",
    "apps/web/vite.config.ts",
    "docs/user/browser-import.md",
    "packages/client-runtime/src/rpc/session.test.ts",
    "packages/client-runtime/src/rpc/session.ts",
    "packages/contracts/src/browserProfile.test.ts",
    "packages/contracts/src/browserProfile.ts",
    "packages/shared/src/authScopeOptions.ts",
    "packages/shared/src/desktopBootstrapToken.test.ts",
    "packages/shared/src/desktopBootstrapToken.ts"
  ],
  "sharedFiles": [
    "knip.jsonc",
    "apps/desktop/src/ipc/DesktopIpcHandlers.ts",
    "apps/desktop/src/ipc/channels.ts",
    "apps/desktop/src/main.ts",
    "apps/desktop/src/preload.ts",
    "apps/server/src/auth/EnvironmentAuth.test.ts",
    "apps/server/src/auth/EnvironmentAuth.ts",
    "apps/server/src/auth/PairingGrantStore.test.ts",
    "apps/server/src/observability/RpcInstrumentation.ts",
    "apps/server/src/server.ts",
    "apps/server/src/ws.ts",
    "apps/web/src/components/settings/IntegrationsSettings.tsx",
    "apps/web/src/components/featureMap/featureMap.mermaid.test.tsx",
    "docs/fork.md",
    "docs/map.md",
    "packages/contracts/src/auth.ts",
    "packages/contracts/src/clientRpcPermissions.ts",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/ipc.ts",
    "packages/contracts/src/rpc.ts",
    "packages/contracts/src/settings.ts",
    "packages/shared/package.json"
  ],
  "keywords": ["cookie mirror", "CookieMirror", "ChromeCookieSource", "preview:mirror-cookies"]
}
```

## usable-secret-requests

```json
{
  "id": "usable-secret-requests",
  "purpose": "request_secret asks for 1-5 named fields on one card and returns a private 0600 file per field under the environment's state directory, plus a one-use secretRef for one-field requests. Answers run one at a time, values are kept exactly as typed, and files expire through the existing secret sweep. Drop when upstream returns usable secret values to agents.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/550"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/551"],
  "newFiles": [],
  "upstreamFiles": [
    "apps/mobile/src/features/threads/SecretRequestCard.tsx",
    "apps/server/src/secrets/SecretRequests.test.ts",
    "apps/server/src/secrets/SecretRequests.ts",
    "apps/web/src/components/chat/SecretRequestCard.tsx",
    "packages/client-runtime/src/secretRequest.test.ts",
    "packages/client-runtime/src/secretRequest.ts",
    "packages/contracts/src/orchestratorMcp.test.ts",
    "packages/contracts/src/secretRequest.ts"
  ],
  "sharedFiles": [
    "apps/server/src/mcp/OrchestratorMcpService.ts",
    "apps/server/src/mcp/OrchestratorMcpToolkit.integration.test.ts",
    "apps/server/src/mcp/toolkits/orchestrator/tools.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "packages/contracts/src/orchestratorMcp.ts",
    "packages/provider-core/src/server/orchestrationInstructions.ts"
  ],
  "keywords": [
    "request_secret",
    "secretRequestFields",
    "SECRET_REQUEST_SINGLE_FIELD_NAME",
    "secret-requests"
  ]
}
```

## outside-agent-oauth-grants

```json
{
  "id": "outside-agent-oauth-grants",
  "purpose": "Approve renewable outside OAuth clients until revoked, with fixed project-id and operation limits. Drop when upstream supports durable scoped outside-agent grants.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/553"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/558"],
  "newFiles": ["apps/server/src/mcp/outsideGrant.ts", "apps/server/src/mcp/outsideGrant.test.ts"],
  "upstreamFiles": [
    "apps/server/src/auth/McpOAuth.test.ts",
    "apps/server/src/auth/mcpOAuthHttp.ts",
    "apps/web/src/components/auth/ConnectAgentSurface.tsx"
  ],
  "sharedFiles": [
    "apps/server/src/auth/EnvironmentAuth.ts",
    "apps/server/src/auth/McpOAuth.ts",
    "apps/server/src/auth/SessionStore.ts",
    "apps/server/src/auth/SessionStore.test.ts",
    "apps/server/src/mcp/McpInvocationContext.ts",
    "apps/server/src/mcp/McpToolAccess.ts",
    "packages/contracts/src/auth.ts",
    "docs/user/outside-agents.md"
  ],
  "keywords": ["until-revoked", "mcp-refresh", "AuthMcpGrantLimits"]
}
```

## preview-credential-fill

```json
{
  "id": "preview-credential-fill",
  "purpose": "preview_fill_credential fills a login input in a server browser tab from the environment's rbw vault (credentialVault server setting) after an https and item-origin check, and returns only the outcome. The fill puts the tab's opener group in a login: every read is refused and session state shows Login in progress, until preview_end_login closes those tabs. Each attempt appends a value-free audit line. Drop when upstream fills credentials without exposing them to agents.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/557"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/561"],
  "newFiles": [
    "apps/server/src/preview/CredentialAudit.ts",
    "apps/server/src/preview/CredentialFill.test.ts",
    "apps/server/src/preview/CredentialFill.ts",
    "apps/server/src/preview/CredentialLogin.integration.test.ts",
    "apps/server/src/preview/CredentialVault.test.ts",
    "apps/server/src/preview/CredentialVault.ts",
    "packages/contracts/src/previewCredential.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/mcp/PreviewAutomationBroker.ts",
    "apps/server/src/mcp/toolkits/preview/handlers.ts",
    "apps/server/src/preview/ServerBrowserPage.ts"
  ],
  "sharedFiles": [
    "apps/server/src/mcp/toolkits/preview/tools.ts",
    "apps/server/src/preview/ServerBrowser.test.ts",
    "apps/server/src/preview/ServerBrowser.ts",
    "apps/server/src/preview/ServerBrowserContexts.ts",
    "apps/server/src/server.ts",
    "docs/map.md",
    "docs/user/browser-import.md",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/previewAutomation.ts",
    "packages/contracts/src/settings.ts",
    "packages/shared/src/t3McpToolPresentation.ts"
  ],
  "keywords": ["preview_fill_credential", "fillCredential", "credentialVault", "rbw"]
}
```

## remote-secret-requests

```json
{
  "id": "remote-secret-requests",
  "purpose": "Relay private source-client answers to a destination-owned one-use webhook request with durable retirement and receipt recovery.",
  "issues": ["https://github.com/toolboxmd/chromeria/issues/385"],
  "prs": ["https://github.com/toolboxmd/chromeria/pull/583"],
  "newFiles": [
    "apps/server/src/peer/PeerAuthority.remoteSecrets.workflow.test.ts",
    "apps/server/src/secrets/RemoteSecretRelay.ts",
    "apps/server/src/secrets/RemoteSecretRequests.ts",
    "apps/server/src/secrets/SecretRequestIngress.ts",
    "apps/server/src/secrets/remoteSecretPersistence.ts",
    "packages/contracts/src/remoteSecret.ts"
  ],
  "upstreamFiles": [
    "apps/server/src/mcp/McpRequestReceipts.ts",
    "apps/server/src/mcp/McpRouting.ts",
    "apps/server/src/peer/PeerAuthority.ts",
    "apps/server/src/peer/PeerAuthority.workflow.fixture.ts",
    "apps/server/src/peer/PeerForwarding.ts",
    "apps/server/src/peer/PeerLinks.ts",
    "apps/server/src/peer/PeerMcpClient.ts",
    "apps/server/src/peer/PeerToolRouting.test.ts",
    "apps/server/src/scheduledTasks/DestinationAutomationAuthority.ts",
    "apps/server/src/scheduledTasks/mutationReceipts.ts"
  ],
  "sharedFiles": [
    "apps/mobile/src/features/threads/SecretRequestCard.tsx",
    "apps/server/src/mcp/McpHttpServer.ts",
    "apps/server/src/mcp/OrchestratorMcpService.ts",
    "apps/server/src/mcp/toolkits/orchestrator/tools.ts",
    "apps/server/src/orchestration-v2/DelegatedTaskQuestionReactor.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "apps/server/src/scheduledTasks/ScheduledTaskService.ts",
    "apps/server/src/secrets/SecretRequests.ts",
    "apps/server/src/server.ts",
    "apps/server/src/ws.ts",
    "apps/web/src/components/chat/SecretRequestCard.tsx",
    "packages/client-runtime/src/secretRequest.ts",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "packages/contracts/src/orchestratorMcp.ts",
    "packages/contracts/src/rpc.ts",
    "packages/contracts/src/scheduledTask.ts"
  ],
  "keywords": ["remote_secret", "RemoteSecret", "secret385"]
}
```
