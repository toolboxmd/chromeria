import { DEFAULT_CLIENT_SETTINGS, DEFAULT_UNIFIED_SETTINGS } from "@t3tools/contracts";
import { StrictMode } from "react";
import { render, stubGlobal } from "../../../test/render";
import { describe, expect, it, vi } from "vite-plus/test";
import "../../index.css";

const { listBrowserImportSources } = vi.hoisted(() => ({
  listBrowserImportSources: vi.fn().mockResolvedValue([]),
}));

vi.mock(import("../preview/previewBridge"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    previewBridge: { ...actual.previewBridge, listBrowserImportSources } as never,
  };
});
vi.mock(import("../../env"), async (original) => ({ ...(await original()), isElectron: true }));
vi.mock(import("../../state/environments"), async (original) => ({
  ...(await original()),
  useEnvironments: (() => ({ environments: [], isReady: true })) as never,
  usePrimaryEnvironment: () => null,
  usePrimaryEnvironmentId: () => null,
}));
vi.mock(import("../../hooks/useSettings"), async (original) => ({
  ...(await original()),
  useClientSettings: ((selector?: (settings: typeof DEFAULT_CLIENT_SETTINGS) => unknown) =>
    selector ? selector(DEFAULT_CLIENT_SETTINGS) : DEFAULT_CLIENT_SETTINGS) as never,
  useClientSettingsHydrated: () => true,
  adoptDesktopBrowserProfiles: async () => undefined,
  usePrimarySettingsAvailable: () => true,
  usePrimarySettings: (() => DEFAULT_UNIFIED_SETTINGS) as never,
  useUpdatePrimarySettings: (() => vi.fn()) as never,
}));
vi.mock(import("./ProjectDefaultsSettings"), async (original) => ({
  ...(await original()),
  ProjectDefaultsSettings: (() => null) as never,
}));
vi.mock(import("./SettingsScopeContext"), async (original) => ({
  ...(await original()),
  useSettingsScope: (() => ({
    scope: { kind: "all", environmentIds: [] },
    search: {},
    environment: null,
    environments: [],
    target: null,
    connectedEnvironments: [],
    targets: [],
  })) as never,
  useOptionalSettingsScope: () => null,
}));

import { BrowserSettingsPanel } from "./IntegrationsSettings";

describe("Integrations browser discovery", () => {
  it("shows every device-bound site reported by desktop main in browser settings", async () => {
    listBrowserImportSources.mockClear();
    using _bridge = stubGlobal("desktopBridge", {
      getClientSettings: async () => DEFAULT_CLIENT_SETTINGS,
      setClientSettings: async () => undefined,
      getConnectionCatalog: async () => null,
      getLocalEnvironmentBootstraps: () => [],
      cookieMirrorConnection: vi.fn(async () => ({
        connected: false,
        generation: 0,
        profiles: [],
        limitations: [
          { host: "accounts.google.com", reason: "device-bound, not mirrored" },
          { host: "work.example.test", reason: "device-bound, not mirrored" },
        ],
      })),
    });
    await using screen = await render(
      <StrictMode>
        <BrowserSettingsPanel />
      </StrictMode>,
    );
    const { container } = screen;
    await vi.waitFor(() => {
      expect(container.textContent).toContain("accounts.google.com");
      expect(container.textContent).toContain("work.example.test");
      expect(container.textContent).toContain("device-bound, not mirrored");
    });
  });
});
