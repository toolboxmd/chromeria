import { WS_METHODS } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as Atom from "effect/reactivity/Atom";
import type * as EnvironmentRegistry from "../connection/registry.ts";
import { createEnvironmentRpcCommand, createEnvironmentRpcQueryAtomFamily } from "./runtime.ts";

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
