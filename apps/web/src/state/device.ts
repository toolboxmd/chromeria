import { useId } from "react";
import { useAtomValue } from "@effect/atom-react";
import { createDeviceEnvironmentAtoms } from "@t3tools/client-runtime/state/device";
import {
  type DeviceHubAccess,
  resolveDeviceHubAccess,
} from "@t3tools/client-runtime/state/deviceHubAccess";
import type { DeviceServiceState, EnvironmentId, DeviceViewerResource } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/reactivity";

import { connectionAtomRuntime } from "../connection/runtime";
import { appAtomRegistry } from "../rpc/atomRegistry";
import { environmentSession } from "./session";
import { useEnvironmentQuery } from "./query";

export const deviceEnvironment = createDeviceEnvironmentAtoms(connectionAtomRuntime);

const EMPTY_DEVICE_STATE: DeviceServiceState = {
  hosts: [],
  hostStatus: "disabled",
  hostStatuses: {},
  devices: [],
  sessions: [],
  onboardingCompleted: false,
  agentAccessEnabled: false,
  hubBasePath: "/api/device-hub",
  revision: 0,
};

export function useDeviceState(environmentId: EnvironmentId | null): {
  readonly state: DeviceServiceState;
  readonly loaded: boolean;
} {
  const query = useEnvironmentQuery(
    environmentId === null ? null : deviceEnvironment.state({ environmentId, input: {} }),
  );
  return { state: query.data ?? EMPTY_DEVICE_STATE, loaded: query.data !== undefined };
}

const deviceHubAccessAtom = Atom.family((environmentId: EnvironmentId) =>
  connectionAtomRuntime
    .atom((get) => {
      const prepared = Option.getOrNull(
        get(environmentSession.preparedConnectionValueAtom(environmentId)),
      );
      if (prepared === null) return Effect.never;
      return resolveDeviceHubAccess({ prepared, hubBasePath: EMPTY_DEVICE_STATE.hubBasePath });
    })
    .pipe(Atom.setIdleTTL(60_000), Atom.withLabel(`device-hub-access:${environmentId}`)),
);

export function useDeviceHubAccessState(
  environmentId: EnvironmentId | null,
  hostId = "local",
  resource?: DeviceViewerResource,
  visible = true,
) {
  const viewerKey = useId();
  const input = resource === undefined ? undefined : { resource, operation: "control" as const };
  const canControl = useAtomValue(
    deviceEnvironment.viewerOpen.permissionAtom(environmentId, input),
  );
  const viewerInput =
    resource === undefined
      ? undefined
      : { resource, operation: canControl ? ("control" as const) : ("view" as const) };
  type AccessResult =
    | Atom.Type<ReturnType<typeof deviceHubAccessAtom>>
    | Atom.Type<ReturnType<typeof deviceEnvironment.viewerAccess>>;
  const query = useEnvironmentQuery<DeviceHubAccess, AsyncResult.AsyncResult.Failure<AccessResult>>(
    environmentId === null || !visible
      ? EMPTY_ACCESS_ATOM
      : viewerInput !== undefined
        ? deviceEnvironment.viewerAccess({ environmentId, input: viewerInput, viewerKey })
        : deviceHubAccessAtom(environmentId),
  );
  const access =
    query.isSuccess && query.data !== null
      ? { ...query.data, query: { ...query.data.query, hostId } }
      : null;
  return { access, error: query.error, refresh: query.refresh };
}

export function useDeviceHubAccess(
  environmentId: EnvironmentId | null,
  hostId = "local",
  resource?: DeviceViewerResource,
  visible = true,
): DeviceHubAccess | null {
  return useDeviceHubAccessState(environmentId, hostId, resource, visible).access;
}

const EMPTY_ACCESS_ATOM = Atom.make(AsyncResult.initial<DeviceHubAccess, never>()).pipe(
  Atom.withLabel("device-hub-access:empty"),
);

export function refreshDeviceHubAccess(environmentId: EnvironmentId): void {
  appAtomRegistry.refresh(deviceHubAccessAtom(environmentId));
}
