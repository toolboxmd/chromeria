import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
} from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

import "~/index.css";

import { render } from "../../../test/render";
import { UsageLimitsPooled } from "./UsageLimitsPooled";

const state = vi.hoisted(() => ({
  prismUseCredits: {} as Record<string, boolean>,
  canWriteSettings: true,
  updateSettings: vi.fn(),
}));

vi.mock("../../hooks/useSettings", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useSettings")>()),
  useEnvironmentSettings: ((_environmentId: unknown, select: (settings: unknown) => unknown) =>
    select({ ...DEFAULT_SERVER_SETTINGS, prismUseCredits: state.prismUseCredits })) as never,
  useUpdateEnvironmentSettings: () => state.updateSettings,
}));

vi.mock("../../state/session", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../state/session")>()),
  useEnvironmentScope: () => state.canWriteSettings,
}));

const checkedAt = "2026-11-26T16:00:00.000Z";
const desk = EnvironmentId.make("desk");

function account(instanceId: string, driver: string, displayName: string): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make(instanceId),
    driver: ProviderDriverKind.make(driver),
    displayName,
    enabled: true,
    installed: true,
    version: "1.0.0",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt,
    models: [],
    slashCommands: [],
    skills: [],
    usageLimits: {
      checkedAt,
      windows: [
        {
          id: "weekly",
          label: "Weekly",
          kind: "weekly",
          usedPercent: 20,
          resetsAt: "2026-11-27T16:00:00.000Z",
        },
      ],
    },
  };
}

function setup(prismUseCredits: Record<string, boolean>, canWriteSettings = true) {
  state.prismUseCredits = prismUseCredits;
  state.canWriteSettings = canWriteSettings;
  state.updateSettings.mockReset();
  const presentations = new Map([
    [
      desk,
      {
        entry: { target: { label: "Desk" } },
        serverConfig: {
          providers: [
            account("codex_work", "codex", "Codex Work"),
            account("claude_work", "claudeAgent", "Claude Work"),
          ],
        },
      },
    ],
  ]);
  return <UsageLimitsPooled presentations={presentations} now={Date.parse(checkedAt)} />;
}

const useCredits = () => page.getByRole("switch", { name: "Use credits" });

it.each([
  { name: "absent", saved: {}, shown: true },
  { name: "on", saved: { codex_work: true }, shown: true },
  { name: "off", saved: { codex_work: false }, shown: false },
  { name: "off for another instance", saved: { codex_personal: false }, shown: true },
])("shows a Codex instance's saved credit use: $name", async ({ saved, shown }) => {
  await using screen = await render(setup(saved));
  await userEvent.click(screen.getByRole("button", { name: /^Codex Work: 80% left/ }));

  await expect.element(useCredits()).toHaveAttribute("aria-checked", shown ? "true" : "false");
  await expect
    .element(useCredits())
    .toHaveAccessibleDescription(
      "When the plan windows are spent, Prism may run Codex on your credits.",
    );
});

it("writes the instance's credit use off with a click and on from the keyboard", async () => {
  await using screen = await render(setup({}));
  await userEvent.click(screen.getByRole("button", { name: /^Codex Work: 80% left/ }));
  await userEvent.click(useCredits());
  expect(state.updateSettings).toHaveBeenLastCalledWith({ prismUseCredits: { codex_work: false } });

  await screen.rerender(setup({ codex_work: false }));
  useCredits().element().focus();
  await userEvent.keyboard(" ");
  expect(state.updateSettings).toHaveBeenCalledExactlyOnceWith({
    prismUseCredits: { codex_work: true },
  });
});

it("offers no credit switch for another provider", async () => {
  await using screen = await render(setup({}));
  await userEvent.click(screen.getByRole("button", { name: /^Claude Work: 80% left/ }));

  await expect.element(page.getByText("Claude Work", { exact: true }).last()).toBeVisible();
  await expect.element(useCredits()).not.toBeInTheDocument();
});

it("disables the switch for a connection that may not change settings", async () => {
  await using screen = await render(setup({}, false));
  await userEvent.click(screen.getByRole("button", { name: /^Codex Work: 80% left/ }));

  await expect.element(useCredits()).toBeDisabled();
});
