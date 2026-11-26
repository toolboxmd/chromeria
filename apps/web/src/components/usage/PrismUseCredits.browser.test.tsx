import { RegistryContext } from "@effect/atom-react";
import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
} from "@t3tools/contracts";
import { AtomRegistry } from "effect/reactivity";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

import "~/index.css";

import { render } from "../../../test/render";
import { environmentPresentations } from "../../state/presentation";
import { UsageLimitsPooled } from "./UsageLimitsPooled";

const state = vi.hoisted(() => ({
  deniedEnvironments: new Set<string>(),
  failingEnvironments: new Set<string>(),
  updateSettings: vi.fn(),
}));

// A kept-alive value atom: each test's registry holds its own environments until the first render.
vi.mock("../../state/presentation", async (importOriginal) => {
  const original = await importOriginal<typeof import("../../state/presentation")>();
  const { Atom } = await import("effect/reactivity");
  return {
    ...original,
    environmentPresentations: {
      ...original.environmentPresentations,
      presentationsWithProvidersAtom: Atom.make(new Map()).pipe(Atom.keepAlive),
    },
  };
});

vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: () => state.updateSettings,
}));

vi.mock("../../state/session", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../state/session")>()),
  useEnvironmentScope: () => true,
  readEnvironmentScope: (environmentId: string) => !state.deniedEnvironments.has(environmentId),
}));

const checkedAt = "2026-11-26T16:00:00.000Z";
const bestia = EnvironmentId.make("bestia");
const rocky = EnvironmentId.make("rocky");
const cavallo = EnvironmentId.make("cavallo");
const owner = { email: "owner@example.com", workspaceId: "ws_owner" };

function account(
  instanceId: string,
  driver: string,
  displayName: string,
  login: { readonly email: string; readonly workspaceId: string } = owner,
): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make(instanceId),
    driver: ProviderDriverKind.make(driver),
    displayName,
    enabled: true,
    installed: true,
    version: "1.0.0",
    status: "ready",
    auth: { status: "authenticated", ...login },
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

const environment = (
  label: string,
  providers: ReadonlyArray<ServerProvider>,
  prismUseCredits: Record<string, boolean> = {},
) => ({
  entry: { target: { label } },
  serverConfig: { providers, settings: { ...DEFAULT_SERVER_SETTINGS, prismUseCredits } },
});

/** One Codex login on three machines, another email on rocky, another workspace on cavallo, and Claude. */
function setup(saved: {
  readonly bestia?: Record<string, boolean>;
  readonly rocky?: Record<string, boolean>;
  readonly cavallo?: Record<string, boolean>;
}) {
  state.deniedEnvironments = new Set();
  state.failingEnvironments = new Set();
  state.updateSettings.mockReset();
  state.updateSettings.mockImplementation(async ({ environmentId }: { environmentId: string }) =>
    state.failingEnvironments.has(environmentId) ? { _tag: "Failure" } : { _tag: "Success" },
  );
  const presentations = new Map([
    [
      bestia,
      environment(
        "Bestia",
        [
          account("codex_work", "codex", "Codex Work"),
          account("claude_work", "claudeAgent", "Claude Work"),
        ],
        saved.bestia,
      ),
    ],
    [
      rocky,
      environment(
        "Rocky",
        [
          account("codex_main", "codex", "Codex Main"),
          account("codex_other", "codex", "Codex Other", {
            email: "other@example.com",
            workspaceId: "ws_owner",
          }),
        ],
        saved.rocky,
      ),
    ],
    [
      cavallo,
      environment(
        "Cavallo",
        [
          account("codex_work", "codex", "Codex Work"),
          account("codex_team", "codex", "Codex Team", {
            email: owner.email,
            workspaceId: "ws_team",
          }),
        ],
        saved.cavallo,
      ),
    ],
  ]);
  const registry = AtomRegistry.make({
    initialValues: [
      [environmentPresentations.presentationsWithProvidersAtom, presentations as never],
    ],
  });
  return (
    <RegistryContext.Provider value={registry}>
      <UsageLimitsPooled
        presentations={new Map([[bestia, presentations.get(bestia)!]])}
        now={Date.parse(checkedAt)}
      />
    </RegistryContext.Provider>
  );
}

const useCredits = () => page.getByRole("switch", { name: "Use credits" });
const patches = () =>
  Object.fromEntries(
    state.updateSettings.mock.calls.map(([call]) => [call.environmentId, call.input.patch]),
  );

it.each([
  { name: "absent everywhere", saved: {}, shown: "true" },
  { name: "on where saved", saved: { bestia: { codex_work: true } }, shown: "true" },
  {
    name: "off on every machine of the login",
    saved: {
      bestia: { codex_work: false },
      rocky: { codex_main: false },
      cavallo: { codex_work: false },
    },
    shown: "false",
  },
  { name: "off on one machine only", saved: { rocky: { codex_main: false } }, shown: "mixed" },
  {
    name: "off for another email and another workspace",
    saved: { rocky: { codex_other: false }, cavallo: { codex_team: false } },
    shown: "true",
  },
])("shows the login's saved credit use: $name", async ({ saved, shown }) => {
  await using screen = await render(setup(saved));
  await userEvent.click(screen.getByRole("button", { name: /^Codex Work: 80% left/ }));

  await expect.element(useCredits()).toHaveAttribute("aria-checked", shown);
  await expect
    .element(useCredits())
    .toHaveAccessibleDescription(
      "When the plan windows are spent, Prism may run Codex on your credits.",
    );
});

it("writes off for the named instance and the same login on every machine, from the keyboard too", async () => {
  await using screen = await render(setup({}));
  await userEvent.click(screen.getByRole("button", { name: /^Codex Work: 80% left/ }));
  await userEvent.click(useCredits());

  expect(patches()).toEqual({
    bestia: { prismUseCredits: { codex_work: false } },
    rocky: { prismUseCredits: { codex_main: false } },
    cavallo: { prismUseCredits: { codex_work: false } },
  });
  await expect.element(page.getByRole("status")).not.toBeInTheDocument();

  state.updateSettings.mockClear();
  useCredits().element().focus();
  await userEvent.keyboard(" ");
  expect(patches()).toMatchObject({ bestia: { prismUseCredits: { codex_work: false } } });
});

it("turns every machine off from the mixed state", async () => {
  await using screen = await render(setup({ rocky: { codex_main: false } }));
  await userEvent.click(screen.getByRole("button", { name: /^Codex Work: 80% left/ }));
  await userEvent.click(useCredits());

  expect(patches()).toEqual({
    bestia: { prismUseCredits: { codex_work: false } },
    rocky: { prismUseCredits: { codex_main: false } },
    cavallo: { prismUseCredits: { codex_work: false } },
  });
});

it("names the machines a failed write or a missing permission left unchanged", async () => {
  await using screen = await render(setup({}));
  state.failingEnvironments.add("rocky");
  state.deniedEnvironments.add("cavallo");
  await userEvent.click(screen.getByRole("button", { name: /^Codex Work: 80% left/ }));
  await userEvent.click(useCredits());

  await expect.element(page.getByText("Not changed on Rocky, Cavallo.")).toBeVisible();
  expect(Object.keys(patches())).toEqual(["bestia", "rocky"]);
});

it("offers no credit switch for another provider", async () => {
  await using screen = await render(setup({}));
  await userEvent.click(screen.getByRole("button", { name: /^Claude Work: 80% left/ }));

  await expect.element(page.getByText("Claude Work", { exact: true }).last()).toBeVisible();
  await expect.element(useCredits()).not.toBeInTheDocument();
});
