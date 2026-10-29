import { useEffect, useId } from "react";
import { useAtomValue } from "@effect/atom-react";
import { PREVIEW_STREAM_BASE_PATH } from "@t3tools/client-runtime/preview/server-browser-stream";
import {
  type DeviceHubAccess,
  resolveDeviceHubAccess,
} from "@t3tools/client-runtime/state/deviceHubAccess";
import type { EnvironmentId, PreviewViewerResource } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";

import { connectionAtomRuntime } from "../connection/runtime";
import { appAtomRegistry } from "../rpc/atomRegistry";
import { environmentSession } from "./session";
import { previewEnvironment } from "./preview";
import { useEnvironmentQuery } from "./query";

const ACCESS_RETRY_INTERVAL_MS = 10_000;

const previewStreamAccessAtom = Atom.family((environmentId: EnvironmentId) =>
  connectionAtomRuntime
    .atom((get) => {
      const prepared = Option.getOrNull(
        get(environmentSession.preparedConnectionValueAtom(environmentId)),
      );
      if (prepared === null) return Effect.never;
      return resolveDeviceHubAccess({ prepared, hubBasePath: PREVIEW_STREAM_BASE_PATH });
    })
    .pipe(Atom.setIdleTTL(60_000), Atom.withLabel(`preview-stream-access:${environmentId}`)),
);

const EMPTY_ACCESS = Atom.make(AsyncResult.initial<DeviceHubAccess, never>());
export function usePreviewStreamAccessState(
  environmentId: EnvironmentId,
  resource?: PreviewViewerResource,
  visible = true,
  retry = visible,
) {
  const viewerKey = useId();
  const canControl = useAtomValue(
    previewEnvironment.viewerOpen.permissionAtom(
      environmentId,
      resource === undefined ? undefined : { resource, operation: "control" },
    ),
  );
  type AccessResult =
    | Atom.Type<ReturnType<typeof previewStreamAccessAtom>>
    | Atom.Type<ReturnType<typeof previewEnvironment.viewerAccess>>;
  const query = useEnvironmentQuery<DeviceHubAccess, AsyncResult.AsyncResult.Failure<AccessResult>>(
    !visible
      ? EMPTY_ACCESS
      : resource === undefined
        ? previewStreamAccessAtom(environmentId)
        : previewEnvironment.viewerAccess({
            environmentId,
            viewerKey,
            input: { resource, operation: canControl ? "control" : "view" },
          }),
  );
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- retries failed access for a visible viewer and releases the timer when its lease changes
  useEffect(() => {
    if (!retry || query.error === null) return;
    const timer = setTimeout(query.refresh, ACCESS_RETRY_INTERVAL_MS);
    return () => clearTimeout(timer);
  }, [query.error, query.refresh, retry]);
  return {
    access: query.isSuccess ? query.data : null,
    error: query.error,
    refresh: query.refresh,
  };
}

export function usePreviewStreamAccess(
  environmentId: EnvironmentId,
  resource?: PreviewViewerResource,
  visible = true,
): DeviceHubAccess | null {
  return usePreviewStreamAccessState(environmentId, resource, visible).access;
}

let floatingViewerSequence = 0;

/** A floating player owns its lease until the player closes, independently of panels. */
export function mountPreviewStreamAccess(
  environmentId: EnvironmentId,
  resource?: PreviewViewerResource,
) {
  type AccessResult =
    | Atom.Type<ReturnType<typeof previewStreamAccessAtom>>
    | Atom.Type<ReturnType<typeof previewEnvironment.viewerAccess>>;
  const atom: Atom.Atom<
    AsyncResult.AsyncResult<DeviceHubAccess, AsyncResult.AsyncResult.Failure<AccessResult>>
  > =
    resource === undefined
      ? previewStreamAccessAtom(environmentId)
      : previewEnvironment.viewerAccess({
          environmentId,
          viewerKey: `picture-in-picture:${++floatingViewerSequence}`,
          input: { resource, operation: "view" },
        });
  const release = appAtomRegistry.mount(atom);
  return {
    release,
    read: (refresh: boolean) => {
      if (refresh) appAtomRegistry.refresh(atom);
      return Effect.runPromise(
        AtomRegistry.getResult(appAtomRegistry, atom, { suspendOnWaiting: true }).pipe(
          Effect.timeout("10 seconds"),
          Effect.orElseSucceed(() => null),
        ),
      );
    },
  };
}
