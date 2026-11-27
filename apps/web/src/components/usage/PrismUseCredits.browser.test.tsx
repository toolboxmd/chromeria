import { RegistryContext } from "@effect/atom-react";
import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
  type ServerProviderCredits,
  UsageLimitSourceId,
} from "@t3tools/contracts";
import { AtomRegistry } from "effect/reactivity";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

import "~/index.css";

import { fakeTimers, render } from "../../../test/render";
import { environmentPresentations } from "../../state/presentation";
import { UsageLimitsPooled } from "./UsageLimitsPooled";

const state = vi.hoisted(() => ({
  deniedEnvironments: new Set<string>(),
  failingEnvironments: new Set<string>(),
  silentEnvironments: new Set<string>(),
  realCommand: false,
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

vi.mock("../../state/use-atom-command", async (importOriginal) => {
  const original = await importOriginal<typeof import("../../state/use-atom-command")>();
  return {
    useAtomCommand: ((...args: Parameters<typeof original.useAtomCommand>) =>
      state.realCommand
        ? original.useAtomCommand(...args)
        : state.updateSettings) as typeof original.useAtomCommand,
  };
});

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

/** A usage hub on bestia that reports the owner's Codex login with a banked reset. */
const hubWithBankedReset = {
  id: UsageLimitSourceId.make("hub"),
  kind: "cliproxy" as const,
  label: "Hub",
  checkedAt,
  accounts: [
    {
      id: "codex-owner@example.com.json",
      driver: ProviderDriverKind.make("codex"),
      ...owner,
      usageLimits: {
        checkedAt,
        windows: [{ id: "weekly", label: "Weekly", kind: "weekly" as const, usedPercent: 20 }],
        resetCredits: { availableCount: 2, nextCreditId: "hub-credit" },
      },
    },
  ],
};

/** One Codex login on three machines, another email on rocky, another workspace on cavallo, and Claude. */
function setup(
  saved: {
    readonly bestia?: Record<string, boolean>;
    readonly rocky?: Record<string, boolean>;
    readonly cavallo?: Record<string, boolean>;
  },
  options: {
    readonly hub?: boolean;
    readonly realCommand?: boolean;
    /** What the usage read of the owner's login reported, by machine. */
    readonly credits?: {
      readonly bestia?: ServerProviderCredits;
      readonly rocky?: ServerProviderCredits;
    };
  } = {},
) {
  const credited = (provider: ServerProvider, credits: ServerProviderCredits | undefined) =>
    credits ? { ...provider, credits } : provider;
  state.deniedEnvironments = new Set();
  state.failingEnvironments = new Set();
  state.silentEnvironments = new Set();
  state.realCommand = options.realCommand ?? false;
  state.updateSettings.mockReset();
  state.updateSettings.mockImplementation(
    ({ environmentId }: { environmentId: string }, call: { signal: AbortSignal }) =>
      state.silentEnvironments.has(environmentId)
        ? // Like the real command, a write nobody answers settles only when its signal aborts.
          new Promise((resolve) =>
            call.signal.addEventListener("abort", () => resolve({ _tag: "Failure" })),
          )
        : Promise.resolve(
            state.failingEnvironments.has(environmentId)
              ? { _tag: "Failure" }
              : { _tag: "Success" },
          ),
  );
  const presentations = new Map([
    [
      bestia,
      environment(
        "Bestia",
        [
          credited(account("codex_work", "codex", "Codex Work"), options.credits?.bestia),
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
          credited(account("codex_main", "codex", "Codex Main"), options.credits?.rocky),
          // Another login's credits never show on the owner's account.
          credited(
            account("codex_other", "codex", "Codex Other", {
              email: "other@example.com",
              workspaceId: "ws_owner",
            }),
            { readAt: "2026-11-26T15:59:00.000Z", balance: 999 },
          ),
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
  const shown = options.hub
    ? {
        ...presentations.get(bestia)!,
        serverConfig: {
          ...presentations.get(bestia)!.serverConfig,
          usageLimitSources: [hubWithBankedReset],
        },
      }
    : presentations.get(bestia)!;
  const registry = AtomRegistry.make({
    initialValues: [
      [environmentPresentations.presentationsWithProvidersAtom, presentations as never],
    ],
  });
  return (
    <RegistryContext.Provider value={registry}>
      <UsageLimitsPooled presentations={new Map([[bestia, shown]])} now={Date.parse(checkedAt)} />
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

it.each([
  {
    name: "the balance of the freshest read among the login's machines",
    credits: {
      bestia: { readAt: "2026-11-26T15:00:00.000Z", balance: 111 },
      rocky: { readAt: "2026-11-26T15:30:00.000Z", balance: 12_345 },
    },
    line: /^Credits left: 12,345 \(read \d{1,2}:\d{2}.*\)$/,
  },
  {
    name: "unlimited",
    credits: { bestia: { readAt: "2026-11-26T15:30:00.000Z", balance: "unlimited" as const } },
    line: /^Credits: unlimited \(read \d{1,2}:\d{2}.*\)$/,
  },
  { name: "nothing when no read reports credits", credits: {}, line: null },
])("shows the login's credits under the switch: $name", async ({ credits, line }) => {
  await using screen = await render(setup({}, { credits }));
  await userEvent.click(screen.getByRole("button", { name: /^Codex Work: 80% left/ }));

  await expect.element(useCredits()).toBeVisible();
  if (line) await expect.element(page.getByText(line)).toBeVisible();
  else await expect.element(page.getByText(/^Credits/)).not.toBeInTheDocument();
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

it("keeps the switch when a usage hub holds the login's banked reset", async () => {
  await using screen = await render(setup({ rocky: { codex_main: false } }, { hub: true }));
  // The hub's credits show on the account, so its redeem target is the hub, not the instance.
  await userEvent.click(
    screen.getByRole("button", { name: /^Codex Work: 80% left.*2 reset credits banked/ }),
  );

  await expect.element(useCredits()).toHaveAttribute("aria-checked", "mixed");
  await userEvent.click(useCredits());
  expect(patches()).toEqual({
    bestia: { prismUseCredits: { codex_work: false } },
    rocky: { prismUseCredits: { codex_main: false } },
    cavallo: { prismUseCredits: { codex_work: false } },
  });
});

it("is busy while a machine does not answer, then names it after 10 seconds", async () => {
  await using screen = await render(setup({}));
  state.silentEnvironments.add("rocky");
  await userEvent.click(screen.getByRole("button", { name: /^Codex Work: 80% left/ }));
  await expect.element(useCredits()).toBeVisible();
  using _timers = fakeTimers();
  await userEvent.click(useCredits());

  await expect.element(useCredits()).toHaveAttribute("aria-busy", "true");
  await expect.element(useCredits()).toBeDisabled();
  await userEvent.click(useCredits(), { force: true });
  expect(state.updateSettings).toHaveBeenCalledTimes(3);
  await expect.element(page.getByRole("status")).not.toBeInTheDocument();

  await vi.advanceTimersByTimeAsync(9_999);
  await expect.element(page.getByRole("status")).not.toBeInTheDocument();
  await vi.advanceTimersByTimeAsync(1);
  await expect.element(page.getByText("Not changed on Rocky.")).toBeVisible();
  await expect.element(useCredits()).toBeEnabled();
  await expect.element(useCredits()).not.toHaveAttribute("aria-busy");
});

it("names enabled machines that are not connected, through the real settings command", async () => {
  await using screen = await render(setup({}, { realCommand: true }));
  await userEvent.click(screen.getByRole("button", { name: /^Codex Work: 80% left/ }));
  await userEvent.click(useCredits());

  await expect.element(page.getByText("Not changed on Bestia, Rocky, Cavallo.")).toBeVisible();
});

it("offers no credit switch for another provider", async () => {
  await using screen = await render(setup({}));
  await userEvent.click(screen.getByRole("button", { name: /^Claude Work: 80% left/ }));

  await expect.element(page.getByText("Claude Work", { exact: true }).last()).toBeVisible();
  await expect.element(useCredits()).not.toBeInTheDocument();
});
