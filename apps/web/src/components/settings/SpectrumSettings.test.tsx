// @vitest-environment jsdom
import {
  DEFAULT_SERVER_SETTINGS,
  DEFAULT_UNIFIED_SETTINGS,
  DEFAULT_SPECTRUM_SETTINGS,
  EnvironmentId,
  ProviderInstanceId,
  type ServerSettings,
  type ServerSettingsPatch,
} from "@t3tools/contracts";
import { applyServerSettingsPatch } from "@t3tools/shared/serverSettings";
import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { resolveSettingsScope } from "./settingsScope";
import { resolveScopedSettingsTargets } from "./scopedSettings";

type Response = { _tag: "Success"; value: ServerSettings } | { _tag: "Failure" };
const state = vi.hoisted(() => ({
  settings: [] as ServerSettings[],
  persist:
    vi.fn<
      (request: {
        environmentId: EnvironmentId;
        input: { patch: ServerSettingsPatch };
      }) => Promise<Response>
    >(),
}));
const ids = [EnvironmentId.make("one"), EnvironmentId.make("two")];
function scopeContext() {
  const environments = ids.map((environmentId, index) => ({
    environmentId,
    label: String(environmentId),
    connection: { phase: "connected" as const },
    serverConfig: {
      settings: state.settings[index]!,
      environment: { capabilities: { projectSettingsOverrides: true } },
    },
  }));
  const scope = resolveSettingsScope({}, [], environments);
  const targets = resolveScopedSettingsTargets(scope, environments);
  return { scope, environments, targets, target: targets[0] ?? null };
}
vi.mock("./SettingsScopeContext", () => ({ useSettingsScope: () => scopeContext() }));
vi.mock("./useScopedSettings", () => ({
  useScopedSettings: () => ({ ...DEFAULT_UNIFIED_SETTINGS, ...state.settings[0] }),
  useScopedSettingsWriteAllowed: () => true,
}));
vi.mock("./useScopedModelAvailability", () => ({ useScopedModelDisabledReason: () => () => null }));
vi.mock("../../state/environments", () => ({ useEnvironmentProviders: () => new Map() }));
vi.mock("../../state/server", () => ({
  EMPTY_SERVER_PROVIDERS: [],
  serverEnvironment: { updateSettings: {} },
}));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => state.persist }));
// Keep the real form and persistence planner; the layout's router/environment chrome is irrelevant here.
vi.mock("./settingsLayout", () => ({
  SettingsPageContainer: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  SettingsSection: ({
    children,
    headerAction,
  }: {
    children: ReactNode;
    headerAction: ReactNode;
  }) => (
    <section>
      {headerAction}
      {children}
    </section>
  ),
}));
import { SpectrumSettings } from "./SpectrumSettings";
let root: Root;
let container: HTMLDivElement;
const render = () => act(() => root.render(<SpectrumSettings />));
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      disconnect() {}
      unobserve() {}
    },
  );
  const primary = { instanceId: ProviderInstanceId.make("codex"), model: "sol" };
  const spectrum = {
    ...DEFAULT_SPECTRUM_SETTINGS,
    slots: [
      { label: "Blue", primary, fallbacks: [] },
      { label: "Red", primary, fallbacks: [] },
    ],
    white: { primary: { ...primary, model: "judge" }, fallbacks: [] },
    relayRounds: 4,
  };
  state.settings = [
    { ...DEFAULT_SERVER_SETTINGS, spectrum },
    { ...DEFAULT_SERVER_SETTINGS, spectrum },
  ];
  state.persist.mockReset();
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(() => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});
it("retains an edited draft when the displayed environment ignores the setting", async () => {
  state.persist.mockImplementation(async ({ environmentId, input }) => {
    if (environmentId === ids[0]) {
      return { _tag: "Success", value: state.settings[0]! };
    }
    state.settings[1] = applyServerSettingsPatch(state.settings[1]!, input.patch);
    return { _tag: "Success", value: state.settings[1]! };
  });
  await render();
  const rounds = container.querySelector<HTMLInputElement>("#spectrum-rounds")!;
  await act(() => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(rounds, "3");
    rounds.dispatchEvent(new Event("input", { bubbles: true }));
  });
  const save = [...container.querySelectorAll("button")].find(
    (button) => button.textContent === "Save Spectrum settings",
  )!;
  expect(save.disabled).toBe(false);
  await act(() => save.click());
  expect(state.persist).toHaveBeenCalled();
  // White is saved as its own Colors alongside the edited limit.
  expect(state.persist.mock.calls[0]![0].input.patch.spectrum).toMatchObject({
    relayRounds: 3,
    white: { primary: { model: "judge" }, fallbacks: [] },
  });
  expect(rounds.value).toBe("3");
  expect(save.disabled).toBe(false);
  expect(container.querySelector('[role="alert"]')?.textContent).toContain("one");
});
it("adopts the displayed environment's reset when another environment fails later", async () => {
  let failOther: ((response: Response) => void) | undefined;
  state.persist.mockImplementation(async ({ environmentId, input }) => {
    if (environmentId === ids[0]) {
      state.settings[0] = applyServerSettingsPatch(state.settings[0]!, input.patch);
      await render();
      return { _tag: "Success", value: state.settings[0]! };
    }
    return new Promise<Response>((resolve) => {
      failOther = resolve;
    });
  });
  await render();
  expect(container.querySelector<HTMLInputElement>("#spectrum-rounds")?.value).toBe("4");
  await act(() =>
    [...container.querySelectorAll("button")]
      .find((button) => button.textContent === "Reset Spectrum settings")!
      .click(),
  );
  expect(state.settings[0]?.spectrum.slots).toHaveLength(0);
  await act(() => {
    failOther?.({ _tag: "Failure" });
  });
  expect(container.querySelector<HTMLInputElement>("#spectrum-rounds")?.value).toBe("2");
  expect(container.querySelectorAll('[id^="spectrum-label-"]')).toHaveLength(0);
  expect(container.querySelector('[role="alert"]')?.textContent).toContain("two");
});
