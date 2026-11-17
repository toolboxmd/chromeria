import { type PeerGrant, WS_METHODS } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as Atom from "effect/reactivity/Atom";
import type * as EnvironmentRegistry from "../connection/registry.ts";
import {
  type AtomCommandResult,
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "./runtime.ts";

export type PeerGrantPermission =
  | "resourceScope"
  | "onward"
  | "preview"
  | "device"
  | "browserExport";

export function isPeerGrantPermissionEnabled(grant: PeerGrant, key: PeerGrantPermission) {
  return key === "resourceScope" ? grant.resourceScope === "environment-resources" : grant[key];
}

export function setPeerGrantPermission<G extends PeerGrant>(
  grant: G,
  key: PeerGrantPermission,
  enabled: boolean,
): G {
  return key === "resourceScope"
    ? { ...grant, resourceScope: enabled ? "environment-resources" : "created-work" }
    : { ...grant, [key]: enabled };
}

/** Shows `next` at once and saves it; a failed save shows `previous` again. Interruptions stay silent. */
export async function autosavePeerGrant<G, A, E>(input: {
  readonly previous: G;
  readonly next: G;
  readonly show: (grant: G) => void;
  readonly save: (grant: G) => Promise<AtomCommandResult<A, E>>;
  readonly onError: (error: unknown) => void;
}) {
  input.show(input.next);
  const result = await input.save(input.next);
  if (result._tag === "Success") return;
  input.show(input.previous);
  if (!isAtomCommandInterrupted(result)) input.onError(squashAtomCommandFailure(result));
}

export function createPeerGrantState<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry.EnvironmentRegistry | R, E>,
) {
  const list = createEnvironmentRpcQueryAtomFamily(runtime, {
    label: "peer-grants:list",
    tag: WS_METHODS.peerGrantsList,
  });
  const update = createEnvironmentRpcCommand(runtime, {
    label: "peer-grants:update",
    tag: WS_METHODS.peerGrantsUpdate,
    onSuccess: (target, registry) =>
      Effect.sync(() => registry.refresh(list({ environmentId: target.environmentId, input: {} }))),
  });
  return { list, update };
}
