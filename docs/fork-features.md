# Fork feature map

Only seven carried features and the V2 data foundation belong to this integration stack.
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
