import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  type LoadedCreditUseEnvironment,
  creditUseRow,
  creditUseSwitches,
  creditUseTargets,
  storedCreditUse,
} from "./PrismUseCredits.logic";

const instanceId = ProviderInstanceId.make("codex_work");

function environment(
  input: {
    readonly phase?: string;
    readonly stored?: boolean;
    readonly holdsSetting?: boolean;
    readonly providers?: ReadonlyArray<ServerProvider>;
  } = {},
): LoadedCreditUseEnvironment {
  return {
    entry: { target: { label: "Alder" } },
    connection: { phase: input.phase ?? "connected" },
    serverConfig: {
      providers: input.providers ?? [],
      settings: {
        ...DEFAULT_SERVER_SETTINGS,
        prismUseCredits: input.stored === undefined ? {} : { [instanceId]: input.stored },
      },
      ...(input.holdsSetting === false ? {} : { prismUseCreditsSetting: true }),
    },
  };
}

const settled = environment({ stored: false });
const row = (
  input: {
    readonly environment?: LoadedCreditUseEnvironment;
    readonly mayWrite?: boolean;
    readonly writing?: boolean;
  } = {},
) =>
  creditUseRow({
    environment: input.environment ?? settled,
    instanceId,
    mayWrite: input.mayWrite ?? true,
    writing: input.writing ?? null,
  });

const off = { checked: false, busy: false, writes: true };
const on = { checked: true, busy: false, writes: false };

describe("creditUseRow", () => {
  it.each([
    {
      line: "1: not connected",
      input: { environment: environment({ phase: "reconnecting", stored: false }) },
      shows: { switch: null, note: "notConnected" },
    },
    {
      line: "2: the server does not send the signal, whatever it stores",
      input: { environment: environment({ holdsSetting: false, stored: false }) },
      shows: { switch: null, note: "notAvailable" },
    },
    {
      line: "2: a missing value on such a server is not on",
      input: { environment: environment({ holdsSetting: false }) },
      shows: { switch: null, note: "notAvailable" },
    },
    {
      line: "3: no permission, stored off",
      input: { mayWrite: false },
      shows: { switch: { checked: false, busy: false, writes: null }, note: "noPermission" },
    },
    {
      line: "3: no permission, nothing stored",
      input: { environment: environment(), mayWrite: false },
      shows: { switch: { checked: true, busy: false, writes: null }, note: "noPermission" },
    },
    {
      line: "4: a write under way shows the value being written",
      input: { writing: true },
      shows: { switch: { checked: true, busy: true, writes: null }, note: null },
    },
    { line: "5: stored off", input: {}, shows: { switch: off, note: null } },
    {
      line: "5: stored on",
      input: { environment: environment({ stored: true }) },
      shows: { switch: on, note: null },
    },
    {
      line: "5: nothing stored is on",
      input: { environment: environment() },
      shows: { switch: on, note: null },
    },
  ] as const)("line $line", ({ input, shows }) => {
    expect(row(input)).toEqual(shows);
  });

  it.each([
    {
      order: "1 before 2",
      input: { environment: environment({ phase: "offline", holdsSetting: false }) },
      note: "notConnected",
    },
    {
      order: "2 before 3",
      input: { environment: environment({ holdsSetting: false }), mayWrite: false },
      note: "notAvailable",
    },
    {
      order: "1 before 4",
      input: { environment: environment({ phase: "reconnecting" }), writing: false },
      note: "notConnected",
    },
    { order: "3 before 4", input: { mayWrite: false, writing: true }, note: "noPermission" },
  ] as const)("the earlier line decides: $order", ({ input, note }) => {
    expect(row(input).note).toBe(note);
  });
});

describe("storedCreditUse", () => {
  it.each([
    { name: "stored off", config: environment({ stored: false }), value: false },
    { name: "nothing stored", config: environment(), value: true },
    {
      name: "a server without the signal, nothing stored",
      config: environment({ holdsSetting: false }),
      value: null,
    },
    {
      name: "a server without the signal, stored off",
      config: environment({ holdsSetting: false, stored: false }),
      value: null,
    },
  ])("only a server that sends the signal tells a value: $name", ({ config, value }) => {
    expect(storedCreditUse(config.serverConfig, instanceId)).toBe(value);
  });
});

describe("creditUseSwitches", () => {
  const provider = (id: string, displayName: string): ServerProvider => ({
    instanceId: ProviderInstanceId.make(id),
    driver: ProviderDriverKind.make("codex"),
    displayName,
    enabled: true,
    installed: true,
    version: "1.0.0",
    status: "ready",
    auth: { status: "authenticated", email: "owner@example.com", workspaceId: "ws_owner" },
    checkedAt: "2026-11-26T16:00:00.000Z",
    models: [],
    slashCommands: [],
    skills: [],
  });

  it("gives a machine with two instances of the login one named switch each", () => {
    const alder = EnvironmentId.make("alder");
    const birch = EnvironmentId.make("birch");
    const environments = new Map([
      [
        alder,
        environment({
          providers: [provider("codex_work", "Codex Work"), provider("codex_copy", "Codex Copy")],
        }),
      ],
      [
        birch,
        {
          ...environment({ providers: [provider("codex_main", "Codex Main")] }),
          entry: { target: { label: "Birch" } },
        },
      ],
    ]);
    const targets = creditUseTargets(environments, {
      key: "alder:codex_work",
      environments: [{ environmentId: alder }],
    });

    expect(creditUseSwitches(environments, targets)).toMatchObject([
      { environmentId: alder, instanceId: "codex_work", label: "Alder · Codex Work" },
      { environmentId: alder, instanceId: "codex_copy", label: "Alder · Codex Copy" },
      { environmentId: birch, instanceId: "codex_main", label: "Birch" },
    ]);
  });
});
