import { RELAY_TUNNEL_RELEASED_MESSAGE, type Discovery } from "@t3tools/client-runtime/relay";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import { EnvironmentId, ORCHESTRATION_PROTOCOL_VERSION } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/reactivity";
import { act, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

type DiscoveredEnvironments = Discovery.RelayEnvironmentDiscoveryState["environments"];

const discovery = vi.hoisted(() => ({
  state: null as Discovery.RelayEnvironmentDiscoveryState | null,
  listeners: new Set<() => void>(),
  refresh: vi.fn<() => Promise<AtomCommandResult<void, never>>>(),
  register: vi.fn(),
  listEnvironments: vi.fn<() => Promise<DiscoveredEnvironments>>(),
}));

vi.mock(import("~/state/use-atom-command"), async (original) => {
  const { relayEnvironmentDiscovery } = await import("~/state/relay");
  return {
    ...(await original()),
    useAtomCommand: ((command: unknown) =>
      command === relayEnvironmentDiscovery.refresh
        ? discovery.refresh
        : discovery.register) as never,
  };
});
vi.mock(import("~/state/environments"), async (original) => {
  const { useSyncExternalStore } = await import("react");
  const subscribe = (listener: () => void) => {
    discovery.listeners.add(listener);
    return () => discovery.listeners.delete(listener);
  };
  const read = () => {
    if (discovery.state === null) throw new Error("Discovery fixture is not initialized");
    return discovery.state;
  };
  return {
    ...(await original()),
    useRelayEnvironmentDiscovery: () => useSyncExternalStore(subscribe, read, read),
  };
});

import { CloudEnvironmentConnectRows } from "./CloudEnvironmentConnectList";

const newMachineId = EnvironmentId.make("new-computer");
const linkedMachines: DiscoveredEnvironments = new Map([
  [
    newMachineId,
    {
      environment: {
        environmentId: newMachineId,
        label: "Work laptop",
        endpoint: {
          httpBaseUrl: "https://relay.example.test",
          wsBaseUrl: "wss://relay.example.test/ws",
          providerKind: "cloudflare_tunnel",
        },
        linkedAt: "2026-09-05T12:00:00.000Z",
      },
      availability: "online",
      status: Option.none(),
      error: Option.none(),
    },
  ],
]);

let root: Root | null;
let container: HTMLDivElement;
let visibility: DocumentVisibilityState;

function publish(state: Discovery.RelayEnvironmentDiscoveryState) {
  discovery.state = state;
  for (const listener of discovery.listeners) listener();
}

async function render(element: React.ReactElement) {
  root ??= createRoot(container);
  await act(async () => root!.render(element));
}

async function mount(refreshWhileEmpty = true) {
  await render(
    <CloudEnvironmentConnectRows
      primaryEnvironmentId={null}
      savedEnvironments={[]}
      showSavedEnvironments
      refreshWhileEmpty={refreshWhileEmpty}
      empty={<p>Waiting for your computer to connect.</p>}
    />,
  );
}

const text = () => container.textContent ?? "";
const button = () =>
  [...container.querySelectorAll("button")].find((element) => element.textContent !== "");
const checkbox = () => container.querySelector<HTMLElement>('[role="checkbox"]')!;
const checked = () => checkbox().getAttribute("aria-checked") === "true";
const toggle = () => act(async () => checkbox().click());
function setVisibility(next: DocumentVisibilityState) {
  visibility = next;
  document.dispatchEvent(new Event("visibilitychange"));
}

async function advance(milliseconds: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(milliseconds);
  });
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  visibility = "visible";
  vi.spyOn(document, "visibilityState", "get").mockImplementation(() => visibility);
  container = document.createElement("div");
  document.body.append(container);
  root = null;
  discovery.listeners.clear();
  discovery.state = {
    environments: new Map(),
    listedSession: Option.none(),
    refreshing: false,
    offline: false,
    error: Option.none(),
  };
  discovery.listEnvironments.mockReset().mockResolvedValue(new Map());
  discovery.register.mockReset().mockResolvedValue(AsyncResult.success(undefined));
  discovery.refresh.mockReset().mockImplementation(async () => {
    publish({
      environments: new Map(),
      listedSession: Option.none(),
      refreshing: true,
      offline: false,
      error: Option.none(),
    });
    const environments = await discovery.listEnvironments();
    publish({
      environments,
      listedSession: Option.none(),
      refreshing: false,
      offline: false,
      error: Option.none(),
    });
    return AsyncResult.success(undefined);
  });
});

afterEach(async () => {
  await act(async () => root?.unmount());
  container.remove();
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("cloud onboarding discovery", () => {
  it.each(["manual", "t3_relay"] as const)(
    "does not offer or auto-select a %s publishing link as a new relay",
    async (providerKind) => {
      const base = linkedMachines.get(newMachineId)!;
      discovery.listEnvironments.mockResolvedValue(
        new Map([
          [
            newMachineId,
            {
              ...base,
              environment: {
                ...base.environment,
                endpoint: { ...base.environment.endpoint, providerKind },
              },
            },
          ],
        ]),
      );
      const onChange = vi.fn();
      await render(
        <CloudEnvironmentConnectRows
          primaryEnvironmentId={null}
          savedEnvironments={[]}
          showSavedEnvironments
          selection={{
            autoSelectedComputers: new Set(),
            selectedIds: new Set(),
            onChange,
          }}
          empty={<p>No addable tunnels.</p>}
        />,
      );

      expect(text()).toBe("No addable tunnels.");
      expect(button()).toBeUndefined();
      expect(onChange).not.toHaveBeenCalled();
      expect(discovery.register).not.toHaveBeenCalled();

      await render(
        <CloudEnvironmentConnectRows
          primaryEnvironmentId={null}
          savedEnvironments={[
            {
              environmentId: newMachineId,
              connection: { phase: "connected", error: null, traceId: null },
              relayManaged: true,
            },
          ]}
          showSavedEnvironments
        />,
      );
      expect(text()).toContain("Work laptop");
      expect(button()?.textContent).toBe("Connected");
      expect(discovery.register).not.toHaveBeenCalled();
    },
  );

});
