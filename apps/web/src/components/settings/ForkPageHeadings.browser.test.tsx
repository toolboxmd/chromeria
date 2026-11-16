import { DEFAULT_PRISM_ROLE_KITS, DEFAULT_UNIFIED_SETTINGS } from "@t3tools/contracts";
import { StrictMode } from "react";
import { describe, expect, it, vi } from "vite-plus/test";

import { render } from "../../../test/render";

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
}));
vi.mock(import("./useScopedSettings"), async (original) => ({
  ...(await original()),
  useScopedSettings: (() => ({
    ...DEFAULT_UNIFIED_SETTINGS,
    prismRoles: DEFAULT_PRISM_ROLE_KITS,
  })) as never,
  useScopedSettingsWriteAllowed: (() => true) as never,
}));
vi.mock(import("./useScopedModelAvailability"), async (original) => ({
  ...(await original()),
  useScopedModelDisabledReason: (() => () => null) as never,
}));
vi.mock(import("../../state/environments"), async (original) => ({
  ...(await original()),
  useEnvironments: (() => ({ environments: [], isReady: true })) as never,
  useEnvironmentProviders: (() => new Map()) as never,
}));
vi.mock(import("../../state/server"), async (original) => ({
  ...(await original()),
  EMPTY_SERVER_PROVIDERS: [],
  serverEnvironment: { updateSettings: {} } as never,
}));
vi.mock(import("../../state/use-atom-command"), async (original) => ({
  ...(await original()),
  useAtomCommand: (() => vi.fn()) as never,
}));
vi.mock(import("../../state/entities"), async (original) => ({
  ...(await original()),
  useThreadShells: (() => []) as never,
}));

import { PrismSettings } from "./PrismSettings";
import { PromachosSettings } from "./PromachosSettings";
import { SpectrumSettings } from "./SpectrumSettings";

describe("fork page headings", () => {
  it.each([
    { title: "Prism (Model Router)", page: <PrismSettings /> },
    { title: "Promachos", page: <PromachosSettings /> },
    { title: "Spectrum", page: <SpectrumSettings /> },
  ])("marks the $title heading as a Chromeria feature", async ({ title, page }) => {
    await using screen = await render(<StrictMode>{page}</StrictMode>);
    const heading = [...screen.container.querySelectorAll("h2")].find((element) =>
      element.textContent?.includes(title),
    );
    expect(heading).toBeDefined();
    expect(
      heading?.parentElement?.querySelector('[role="img"][aria-label="Chromeria feature"]'),
    ).not.toBeNull();
  });
});
