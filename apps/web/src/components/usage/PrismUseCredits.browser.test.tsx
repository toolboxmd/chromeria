import { RegistryContext, useAtomValue } from "@effect/atom-react";
import { applyServerConfigProjection } from "@t3tools/client-runtime/state/server-config-projection";
import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerConfig,
  type ServerProvider,
  type ServerProviderCredits,
  UsageLimitSourceId,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import { type Atom, AtomRegistry } from "effect/reactivity";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

import "~/index.css";

import { fakeTimers, render } from "../../../test/render";
import { environmentPresentations } from "../../state/presentation";
import { UsageLimitsPooled } from "./UsageLimitsPooled";

const state = vi.hoisted(() => ({
  /** Environments whose session may not write settings; each test's registry holds its own. */
  deniedAtom: undefined as unknown as Atom.Writable<ReadonlySet<string>>,
  failingEnvironments: new Set<string>(),
  silentEnvironments: new Set<string>(),
  heldEnvironments: new Set<string>(),
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

// The permission is an atom, as in the app: a row that only read it once would miss a grant.
vi.mock("../../state/session", async (importOriginal) => {
  const { Atom } = await import("effect/reactivity");
  const nobody: ReadonlySet<string> = new Set();
  state.deniedAtom = Atom.make(nobody).pipe(Atom.keepAlive);
  return {
    ...(await importOriginal<typeof import("../../state/session")>()),
    useEnvironmentScope: () => true,
    useEnvironmentsWithScope: (environments: ReadonlyArray<{ readonly environmentId: string }>) => {
      const denied = useAtomValue(state.deniedAtom);
      return new Set(
        environments.map(({ environmentId }) => environmentId).filter((id) => !denied.has(id)),
      );
    },
  };
});

const checkedAt = "2026-11-26T16:00:00.000Z";
const alder = EnvironmentId.make("alder");
const birch = EnvironmentId.make("birch");
const cedar = EnvironmentId.make("cedar");
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
  holdsSetting = true,
) => ({
  entry: { target: { label } },
  connection: { phase: "connected" },
  serverConfig: {
    providers,
    settings: { ...DEFAULT_SERVER_SETTINGS, prismUseCredits },
    // A server from before the signal sends no such key.
    ...(holdsSetting ? { prismUseCreditsSetting: true } : {}),
  },
});

/** A usage hub on Alder that reports the owner's Codex login with a banked reset. */
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

/**
 * One Codex login on three machines, another email on Birch, another workspace on Cedar, and
 * Claude. The settings command stores a patch as the server does: the answer and the new
 * settings are separate arrivals, and `held` keeps the settings back until `release`.
 */
function setup(
  saved: {
    readonly alder?: Record<string, boolean>;
    readonly birch?: Record<string, boolean>;
    readonly cedar?: Record<string, boolean>;
  } = {},
  options: {
    readonly hub?: boolean;
    readonly realCommand?: boolean;
    /** Only these machines exist. */
    readonly machines?: ReadonlyArray<EnvironmentId>;
    /** Environments whose server does not send the signal: it accepts a patch and reports nothing. */
    readonly withoutSignal?: ReadonlyArray<string>;
    readonly denied?: ReadonlyArray<string>;
    readonly disconnected?: ReadonlyArray<string>;
    /** What the usage read of the owner's login reported, by machine. */
    readonly credits?: {
      readonly alder?: ServerProviderCredits;
      readonly birch?: ServerProviderCredits;
    };
  } = {},
) {
  const credited = (provider: ServerProvider, credits: ServerProviderCredits | undefined) =>
    credits ? { ...provider, credits } : provider;
  const sendsSignal = (environmentId: string) => !options.withoutSignal?.includes(environmentId);
  state.failingEnvironments = new Set();
  state.silentEnvironments = new Set();
  state.heldEnvironments = new Set();
  state.realCommand = options.realCommand ?? false;
  state.updateSettings.mockReset();
  const all = new Map([
    [
      alder,
      environment(
        "Alder",
        [
          credited(account("codex_work", "codex", "Codex Work"), options.credits?.alder),
          // Only the Codex driver reports credits; another driver's never show.
          credited(account("claude_work", "claudeAgent", "Claude Work"), {
            readAt: "2026-11-26T15:59:00.000Z",
            balance: 777,
          }),
        ],
        saved.alder,
        sendsSignal(alder),
      ),
    ],
    [
      birch,
      environment(
        "Birch",
        [
          credited(account("codex_main", "codex", "Codex Main"), options.credits?.birch),
          // Another login's credits never show on the owner's account.
          credited(
            account("codex_other", "codex", "Codex Other", {
              email: "other@example.com",
              workspaceId: "ws_owner",
            }),
            { readAt: "2026-11-26T15:59:00.000Z", balance: 999 },
          ),
        ],
        saved.birch,
        sendsSignal(birch),
      ),
    ],
    [
      cedar,
      environment(
        "Cedar",
        [
          account("codex_work", "codex", "Codex Work"),
          account("codex_team", "codex", "Codex Team", {
            email: owner.email,
            workspaceId: "ws_team",
          }),
        ],
        saved.cedar,
        sendsSignal(cedar),
      ),
    ],
  ]);
  let stored = new Map(
    [...all]
      .filter(([environmentId]) => options.machines?.includes(environmentId) ?? true)
      .map(([environmentId, presentation]) => [
        environmentId,
        options.disconnected?.includes(environmentId)
          ? { ...presentation, connection: { phase: "reconnecting" } }
          : presentation,
      ]),
  );
  const shown = options.hub
    ? {
        ...stored.get(alder)!,
        serverConfig: {
          ...stored.get(alder)!.serverConfig,
          usageLimitSources: [hubWithBankedReset],
        },
      }
    : stored.get(alder)!;
  const registry = AtomRegistry.make({
    initialValues: [
      [environmentPresentations.presentationsWithProvidersAtom, stored as never],
      [state.deniedAtom, new Set(options.denied)],
    ],
  });
  /** New settings arrive from the server, changed by this client or by another. */
  const publish = (environmentId: EnvironmentId, prismUseCredits: Record<string, boolean> = {}) => {
    const current = stored.get(environmentId)!;
    stored = new Map(stored).set(environmentId, {
      ...current,
      serverConfig: {
        ...current.serverConfig,
        settings: {
          ...current.serverConfig.settings,
          prismUseCredits: { ...current.serverConfig.settings.prismUseCredits, ...prismUseCredits },
        },
      },
    });
    // The mock above makes this atom a plain value, which its declared type does not say.
    registry.set(environmentPresentations.presentationsWithProvidersAtom as never, stored as never);
  };
  const store = (environmentId: EnvironmentId, next: object) => {
    stored = new Map(stored).set(environmentId, { ...stored.get(environmentId)!, ...next });
    registry.set(environmentPresentations.presentationsWithProvidersAtom as never, stored as never);
  };
  const held: Array<() => void> = [];
  state.updateSettings.mockImplementation(
    (
      {
        environmentId,
        input,
      }: {
        environmentId: EnvironmentId;
        input: { patch: { prismUseCredits: Record<string, boolean> } };
      },
      call: { signal: AbortSignal },
    ) => {
      if (state.silentEnvironments.has(environmentId)) {
        // Like the real command, a write nobody answers settles only when its signal aborts.
        return new Promise((resolve) =>
          call.signal.addEventListener("abort", () => resolve({ _tag: "Failure" })),
        );
      }
      if (state.failingEnvironments.has(environmentId)) {
        return Promise.resolve({ _tag: "Failure" });
      }
      const report = () => publish(environmentId, input.patch.prismUseCredits);
      if (state.heldEnvironments.has(environmentId)) held.push(report);
      else if (sendsSignal(environmentId)) report();
      return Promise.resolve({ _tag: "Success" });
    },
  );
  return {
    ui: (
      <RegistryContext.Provider value={registry}>
        <UsageLimitsPooled presentations={new Map([[alder, shown]])} now={Date.parse(checkedAt)} />
      </RegistryContext.Provider>
    ),
    publish,
    /** The settings of every acknowledged write reach the client. */
    release: () => held.splice(0).forEach((report) => report()),
    grant: (environmentId: string) =>
      registry.set(
        state.deniedAtom,
        new Set([...registry.get(state.deniedAtom)].filter((id) => id !== environmentId)),
      ),
    revoke: (environmentId: string) =>
      registry.set(state.deniedAtom, new Set([...registry.get(state.deniedAtom), environmentId])),
    connection: (environmentId: EnvironmentId, phase: string) =>
      store(environmentId, { connection: { phase } }),
    /** The server sends its settings again, unchanged, through the client's own config reducer. */
    sameSettingsAgain: (environmentId: EnvironmentId) => {
      const config = stored.get(environmentId)!.serverConfig as never as ServerConfig;
      const event = {
        version: 1,
        type: "settingsUpdated",
        payload: { settings: structuredClone(config.settings) },
      } as const;
      const next = applyServerConfigProjection(
        Option.some({ config, latestEvent: event, source: "live" }),
        event,
      );
      store(environmentId, { serverConfig: Option.getOrThrow(next).config });
    },
  };
}

const description = "When the plan windows are spent, Prism may run Codex on your credits.";
const useCreditsOn = (machine: string) =>
  page.getByRole("switch", { name: `Use credits on ${machine}`, exact: true });
const openCodexWork = (screen: { getByRole: typeof page.getByRole }) =>
  userEvent.click(screen.getByRole("button", { name: /^Codex Work: 80% left/ }));
/** Every write so far, as [environment, patch]. */
const writes = () =>
  state.updateSettings.mock.calls.map(([call]) => [call.environmentId, call.input.patch]);
/** The whole text of the row that carries this note. */
const rowOf = (note: string) =>
  page.getByText(note, { exact: true }).element().parentElement?.parentElement?.textContent;

it.each([
  { name: "absent everywhere", saved: {}, shown: ["true", "true", "true"] },
  {
    name: "off on one machine only",
    saved: { birch: { codex_main: false } },
    shown: ["true", "false", "true"],
  },
  {
    name: "off on two, on where saved",
    saved: {
      alder: { codex_work: false },
      birch: { codex_main: true },
      cedar: { codex_work: false },
    },
    shown: ["false", "true", "false"],
  },
  {
    name: "off for another email and another workspace",
    saved: { birch: { codex_other: false }, cedar: { codex_team: false } },
    shown: ["true", "true", "true"],
  },
])("line 5: one switch per machine with that machine's value: $name", async ({ saved, shown }) => {
  await using screen = await render(setup(saved).ui);
  await openCodexWork(screen);

  for (const [index, machine] of ["Alder", "Birch", "Cedar"].entries()) {
    await expect.element(useCreditsOn(machine)).toHaveAttribute("aria-checked", shown[index]!);
    await expect.element(useCreditsOn(machine)).toHaveAccessibleDescription(description);
  }
  expect(page.getByRole("switch").elements()).toHaveLength(3);
});

/** The time of day as the browser's own formatter writes it, in the test's zone. */
const timeOfDay = (iso: string) =>
  new Intl.DateTimeFormat(undefined, { hour: "numeric", minute: "2-digit" }).format(new Date(iso));

it.each([
  {
    name: "the balance of the freshest read among the login's machines",
    credits: {
      alder: { readAt: "2026-11-26T15:00:00.000Z", balance: 111 },
      birch: { readAt: "2026-11-26T15:30:00.000Z", balance: 12_345 },
    },
    line: `Credits left: 12,345 (read ${timeOfDay("2026-11-26T15:30:00.000Z")})`,
  },
  {
    name: "unlimited",
    credits: { alder: { readAt: "2026-11-26T15:30:00.000Z", balance: "unlimited" as const } },
    line: `Credits: unlimited (read ${timeOfDay("2026-11-26T15:30:00.000Z")})`,
  },
  { name: "nothing when no read answered", credits: {}, line: null },
  {
    name: "nothing once a newer read found none, whatever an older read showed",
    credits: {
      alder: { readAt: "2026-11-26T15:00:00.000Z", balance: 555 },
      birch: { readAt: "2026-11-26T15:30:00.000Z", balance: "none" as const },
    },
    line: null,
  },
  {
    // Birch's usage was read at 16:00 and its reply carried no credits, so it tells nothing.
    name: "an older balance while a newer usage reply on another machine carried no credits",
    credits: { alder: { readAt: "2026-11-26T15:00:00.000Z", balance: 555 } },
    line: `Credits left: 555 (read ${timeOfDay("2026-11-26T15:00:00.000Z")})`,
  },
])("shows the login's credits under the description: $name", async ({ credits, line }) => {
  await using screen = await render(setup({}, { credits }).ui);
  await openCodexWork(screen);

  await expect.element(useCreditsOn("Alder")).toBeVisible();
  if (line) await expect.element(page.getByText(line, { exact: true })).toBeVisible();
  else await expect.element(page.getByText(/^Credits/)).not.toBeInTheDocument();
});

it("writes only the clicked machine, and reaches both values in one click each", async () => {
  await using screen = await render(setup({ cedar: { codex_work: false } }).ui);
  await openCodexWork(screen);

  await userEvent.click(useCreditsOn("Birch"));
  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-checked", "false");
  await userEvent.click(useCreditsOn("Birch"));
  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-checked", "true");
  await userEvent.click(useCreditsOn("Cedar"));
  await expect.element(useCreditsOn("Cedar")).toHaveAttribute("aria-checked", "true");

  expect(writes()).toEqual([
    ["birch", { prismUseCredits: { codex_main: false } }],
    ["birch", { prismUseCredits: { codex_main: true } }],
    ["cedar", { prismUseCredits: { codex_work: true } }],
  ]);
  await expect.element(useCreditsOn("Alder")).toHaveAttribute("aria-checked", "true");
});

it("carries the Chromeria mark on the title, and the keyboard goes row by row", async () => {
  await using screen = await render(setup().ui);
  await openCodexWork(screen);

  const mark = page.getByRole("img", { name: "Chromeria feature" });
  await expect.element(mark).toBeVisible();
  await userEvent.click(mark);
  expect(state.updateSettings).not.toHaveBeenCalled();

  mark.element().focus();
  await userEvent.keyboard("{Tab}");
  await expect.element(useCreditsOn("Alder")).toHaveFocus();
  await userEvent.keyboard("{Tab}");
  await expect.element(useCreditsOn("Birch")).toHaveFocus();
  await userEvent.keyboard(" ");
  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-checked", "false");
  expect(writes()).toEqual([["birch", { prismUseCredits: { codex_main: false } }]]);
});

it("names the machine also when the login is on one machine", async () => {
  await using screen = await render(setup({}, { machines: [alder] }).ui);
  await openCodexWork(screen);

  await expect.element(useCreditsOn("Alder")).toHaveAttribute("aria-checked", "true");
  expect(page.getByRole("switch").elements()).toHaveLength(1);
  // The machine's name is the switch's label.
  await userEvent.click(page.getByText("Alder", { exact: true }).last());
  await expect.element(useCreditsOn("Alder")).toHaveAttribute("aria-checked", "false");
});

it.each([
  { line: 1, options: { disconnected: ["alder"] }, note: "Not connected" },
  {
    line: 2,
    options: { withoutSignal: ["alder"] },
    note: "Not available: update Chromeria on this machine",
  },
])("line $line names a sole machine beside its note", async ({ options, note }) => {
  await using screen = await render(setup({}, { machines: [alder], ...options }).ui);
  await openCodexWork(screen);

  await expect.element(page.getByText(note, { exact: true })).toBeVisible();
  expect(rowOf(note)).toBe(`Alder${note}`);
  await expect.element(page.getByRole("switch")).not.toBeInTheDocument();
});

it("line 1: a machine that is not connected says so and has no switch", async () => {
  await using screen = await render(
    setup({ birch: { codex_main: false } }, { disconnected: ["birch"] }).ui,
  );
  await openCodexWork(screen);

  await expect.element(page.getByText("Not connected")).toBeVisible();
  expect(rowOf("Not connected")).toBe("BirchNot connected");
  await expect.element(useCreditsOn("Birch")).not.toBeInTheDocument();
  await userEvent.click(useCreditsOn("Cedar"));
  expect(writes()).toEqual([["cedar", { prismUseCredits: { codex_work: false } }]]);
});

it("line 2: a machine whose server does not send the signal has no switch, whatever it stores", async () => {
  await using screen = await render(
    setup({ cedar: { codex_work: false } }, { withoutSignal: ["cedar"] }).ui,
  );
  await openCodexWork(screen);

  const note = "Not available: update Chromeria on this machine";
  await expect.element(page.getByText(note)).toBeVisible();
  expect(rowOf(note)).toBe(`Cedar${note}`);
  await expect.element(useCreditsOn("Cedar")).not.toBeInTheDocument();
  expect(page.getByRole("switch").elements()).toHaveLength(2);

  // The others still reach off and on.
  await userEvent.click(useCreditsOn("Alder"));
  await expect.element(useCreditsOn("Alder")).toHaveAttribute("aria-checked", "false");
  await userEvent.click(useCreditsOn("Alder"));
  await expect.element(useCreditsOn("Alder")).toHaveAttribute("aria-checked", "true");
  expect(writes().map(([environmentId]) => environmentId)).toEqual(["alder", "alder"]);
});

it("line 3: without permission the switch shows the value and is disabled, until a grant arrives", async () => {
  const world = setup({ cedar: { codex_work: false } }, { denied: ["cedar"] });
  await using screen = await render(world.ui);
  await openCodexWork(screen);

  await expect.element(useCreditsOn("Cedar")).toHaveAttribute("aria-checked", "false");
  await expect.element(useCreditsOn("Cedar")).toBeDisabled();
  await expect
    .element(useCreditsOn("Cedar"))
    .toHaveAccessibleDescription(`${description} No permission`);
  expect(rowOf("No permission")).toBe("CedarNo permission");
  await userEvent.click(useCreditsOn("Cedar"), { force: true });
  expect(state.updateSettings).not.toHaveBeenCalled();

  // Settings changed elsewhere and a permission granted while the popover is open both show.
  world.publish(alder, { codex_work: false });
  await expect.element(useCreditsOn("Alder")).toHaveAttribute("aria-checked", "false");
  world.grant("cedar");
  await expect.element(useCreditsOn("Cedar")).toBeEnabled();
  await expect.element(page.getByText("No permission")).not.toBeInTheDocument();
  await userEvent.click(useCreditsOn("Cedar"));
  expect(writes()).toEqual([["cedar", { prismUseCredits: { codex_work: true } }]]);
});

it("line 4: an acknowledged write stays busy on the written value until the settings show it", async () => {
  const world = setup();
  await using screen = await render(world.ui);
  state.heldEnvironments.add("birch");
  await openCodexWork(screen);
  await userEvent.click(useCreditsOn("Birch"));

  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-busy", "true");
  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-checked", "false");
  await expect.element(useCreditsOn("Birch")).toBeDisabled();
  await userEvent.click(useCreditsOn("Birch"), { force: true });
  expect(state.updateSettings).toHaveBeenCalledTimes(1);
  // The other machines are not held up.
  await expect.element(useCreditsOn("Alder")).toBeEnabled();
  // Settings that still carry the old value are not the write's answer.
  world.sameSettingsAgain(birch);
  world.publish(birch, { codex_other: false });
  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-busy", "true");

  world.release();
  await expect.element(useCreditsOn("Birch")).toBeEnabled();
  await expect.element(useCreditsOn("Birch")).not.toHaveAttribute("aria-busy");
  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-checked", "false");
  await userEvent.click(useCreditsOn("Birch"));
  expect(writes()).toEqual([
    ["birch", { prismUseCredits: { codex_main: false } }],
    ["birch", { prismUseCredits: { codex_main: true } }],
  ]);
});

it.each([
  {
    name: "the permission is revoked and restored",
    hide: (world: ReturnType<typeof setup>) => world.revoke("birch"),
    hidden: "No permission",
    show: (world: ReturnType<typeof setup>) => world.grant("birch"),
  },
  {
    name: "the machine reconnects",
    hide: (world: ReturnType<typeof setup>) => world.connection(birch, "reconnecting"),
    hidden: "Not connected",
    show: (world: ReturnType<typeof setup>) => world.connection(birch, "connected"),
  },
])("line 4 is still under way after $name", async ({ hide, hidden, show }) => {
  const world = setup();
  await using screen = await render(world.ui);
  state.heldEnvironments.add("birch");
  await openCodexWork(screen);
  await userEvent.click(useCreditsOn("Birch"));
  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-busy", "true");

  hide(world);
  await expect.element(page.getByText(hidden, { exact: true })).toBeVisible();
  show(world);

  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-busy", "true");
  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-checked", "false");
  await userEvent.click(useCreditsOn("Birch"), { force: true });
  expect(writes()).toEqual([["birch", { prismUseCredits: { codex_main: false } }]]);

  world.release();
  await expect.element(useCreditsOn("Birch")).toBeEnabled();
  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-checked", "false");
});

it("line 4 is still under way after a close and reopen", async () => {
  const world = setup();
  await using screen = await render(world.ui);
  state.heldEnvironments.add("birch");
  await openCodexWork(screen);
  await userEvent.click(useCreditsOn("Birch"));
  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-busy", "true");

  await userEvent.keyboard("{Escape}");
  await expect.element(page.getByRole("switch")).not.toBeInTheDocument();
  await openCodexWork(screen);

  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-busy", "true");
  world.release();
  await expect.element(useCreditsOn("Birch")).toBeEnabled();
  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-checked", "false");
});

it.each([
  { name: "nobody answers", hold: () => state.silentEnvironments.add("birch") },
  {
    name: "the answer came but the settings never did",
    hold: () => state.heldEnvironments.add("birch"),
  },
])("line 4 ends after 10 seconds when $name", async ({ hold }) => {
  await using screen = await render(setup().ui);
  hold();
  await openCodexWork(screen);
  await expect.element(useCreditsOn("Birch")).toBeVisible();
  using _timers = fakeTimers();
  await userEvent.click(useCreditsOn("Birch"));

  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-busy", "true");
  await vi.advanceTimersByTimeAsync(9_999);
  await expect.element(useCreditsOn("Birch")).toBeDisabled();
  await vi.advanceTimersByTimeAsync(1);

  // The row is back on the server's actual value, with nothing beside it.
  await expect.element(useCreditsOn("Birch")).toBeEnabled();
  await expect.element(useCreditsOn("Birch")).not.toHaveAttribute("aria-busy");
  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-checked", "true");
  expect(useCreditsOn("Birch").element().parentElement?.parentElement?.textContent).toBe("Birch");
});

it("a failed write leaves the row on its actual value with no note, also after the same settings arrive again", async () => {
  const world = setup();
  await using screen = await render(world.ui);
  state.failingEnvironments.add("birch");
  await openCodexWork(screen);
  await userEvent.click(useCreditsOn("Birch"));

  const row = () => useCreditsOn("Birch").element().parentElement?.parentElement?.textContent;
  await expect.element(useCreditsOn("Birch")).toBeEnabled();
  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-checked", "true");
  expect(row()).toBe("Birch");

  world.sameSettingsAgain(birch);
  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-checked", "true");
  expect(row()).toBe("Birch");
  await expect.element(useCreditsOn("Birch")).toHaveAccessibleDescription(description);

  await userEvent.click(useCreditsOn("Birch"));
  expect(writes()).toEqual([
    ["birch", { prismUseCredits: { codex_main: false } }],
    ["birch", { prismUseCredits: { codex_main: false } }],
  ]);
});

it("shows each machine's actual value again after a close and reopen", async () => {
  await using screen = await render(setup().ui);
  state.failingEnvironments.add("birch");
  await openCodexWork(screen);
  await userEvent.click(useCreditsOn("Alder"));
  await userEvent.click(useCreditsOn("Birch"));
  await expect.element(useCreditsOn("Alder")).toHaveAttribute("aria-checked", "false");

  await userEvent.keyboard("{Escape}");
  await expect.element(page.getByRole("switch")).not.toBeInTheDocument();
  await openCodexWork(screen);

  await expect.element(useCreditsOn("Alder")).toHaveAttribute("aria-checked", "false");
  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-checked", "true");
  state.failingEnvironments.clear();
  await userEvent.click(useCreditsOn("Birch"));
  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-checked", "false");
});

it("keeps the switches when a usage hub holds the login's banked reset", async () => {
  await using screen = await render(setup({ birch: { codex_main: false } }, { hub: true }).ui);
  // The hub's credits show on the account, so its redeem target is the hub, not the instance.
  await userEvent.click(
    screen.getByRole("button", { name: /^Codex Work: 80% left.*2 reset credits banked/ }),
  );

  await expect.element(useCreditsOn("Birch")).toHaveAttribute("aria-checked", "false");
  await userEvent.click(useCreditsOn("Alder"));
  expect(writes()).toEqual([["alder", { prismUseCredits: { codex_work: false } }]]);
});

it("reports a write the real settings command could not deliver the way every setting does", async () => {
  using warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  await using screen = await render(setup({}, { realCommand: true }).ui);
  await openCodexWork(screen);
  await userEvent.click(useCreditsOn("Cedar"));

  await vi.waitFor(() =>
    expect(warn).toHaveBeenCalledWith(
      "[atom-command] server settings update failed",
      expect.anything(),
    ),
  );
  await expect.element(useCreditsOn("Cedar")).toBeEnabled();
  await expect.element(useCreditsOn("Cedar")).toHaveAttribute("aria-checked", "true");
});

it("offers no credit switch and no credits for another provider", async () => {
  await using screen = await render(setup().ui);
  await userEvent.click(screen.getByRole("button", { name: /^Claude Work: 80% left/ }));

  await expect.element(page.getByText("Claude Work", { exact: true }).last()).toBeVisible();
  await expect.element(page.getByRole("switch")).not.toBeInTheDocument();
  await expect.element(page.getByText(/^Credits/)).not.toBeInTheDocument();
});
