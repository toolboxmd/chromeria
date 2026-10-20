import { useAtomValue } from "@effect/atom-react";
import { createDeviceEnvironmentAtoms } from "@t3tools/client-runtime/state/device";
import { resolveDeviceHubAccess } from "@t3tools/client-runtime/state/deviceHubAccess";
import type { DeviceViewerResource, EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/reactivity";
import { useId, useMemo } from "react";

import { connectionAtomRuntime } from "../connection/runtime";
import { appAtomRegistry } from "./atom-registry";
import { useEnvironmentQuery } from "./query";
import { environmentSession, usePreparedConnection } from "./session";

export const deviceEnvironment = createDeviceEnvironmentAtoms(connectionAtomRuntime);

const deviceHubAccessAtom = Atom.family((environmentId: EnvironmentId) =>
  connectionAtomRuntime
    .atom((get) => {
      const prepared = Option.getOrNull(
        get(environmentSession.preparedConnectionValueAtom(environmentId)),
      );
      return prepared === null
        ? Effect.never
        : resolveDeviceHubAccess({ prepared, hubBasePath: "/api/device-hub" });
    })
    .pipe(Atom.setIdleTTL(60_000), Atom.withLabel(`mobile-device-hub-access:${environmentId}`)),
);

export function refreshDeviceHubAccess(environmentId: EnvironmentId) {
  appAtomRegistry.refresh(deviceHubAccessAtom(environmentId));
}

const EMPTY_ACCESS = Atom.make(
  AsyncResult.initial<
    import("@t3tools/client-runtime/state/deviceHubAccess").DeviceHubAccess,
    never
  >(),
);
export function useDeviceHubAccess(
  environmentId: EnvironmentId,
  hostId: string,
  resource?: DeviceViewerResource,
  visible = true,
) {
  const viewerKey = useId();
  const prepared = usePreparedConnection(environmentId);
  const canControl = useAtomValue(
    deviceEnvironment.viewerOpen.permissionAtom(
      environmentId,
      resource === undefined ? undefined : { resource, operation: "control" },
    ),
  );
  type AccessResult =
    | Atom.Type<ReturnType<typeof deviceHubAccessAtom>>
    | Atom.Type<ReturnType<typeof deviceEnvironment.viewerAccess>>;
  const query = useEnvironmentQuery<
    import("@t3tools/client-runtime/state/deviceHubAccess").DeviceHubAccess,
    AsyncResult.AsyncResult.Failure<AccessResult>
  >(
    !visible
      ? EMPTY_ACCESS
      : resource === undefined
        ? deviceHubAccessAtom(environmentId)
        : deviceEnvironment.viewerAccess({
            environmentId,
            viewerKey,
            input: { resource, operation: canControl ? "control" : "view" },
          }),
  );
  const access = useMemo(
    () =>
      query.isSuccess && query.data && Option.isSome(prepared)
        ? { ...query.data, query: { ...query.data.query, hostId } }
        : null,
    [query.isSuccess, query.data, prepared, hostId],
  );
  return { access, error: query.error, refresh: query.refresh };
}
