import { createEnvironmentRpcQueryAtomFamily } from "@t3tools/client-runtime/state/runtime";
import { type EnvironmentId, FEATURE_MAP_WS_METHODS, type ProjectId } from "@t3tools/contracts";

import { connectionAtomRuntime } from "../connection/runtime";
import { useEnvironmentQuery } from "./query";

/** Fork: a project's feature map (toolboxmd/chromeria#235); the open Map tab refreshes it. */
const featureMapRead = createEnvironmentRpcQueryAtomFamily(connectionAtomRuntime, {
  label: "environment-data:feature-map:read",
  tag: FEATURE_MAP_WS_METHODS.read,
  staleTimeMs: 30_000,
});

/** The map read through `environmentId`; data stays the last good snapshot when a read fails. */
export function useFeatureMap(environmentId: EnvironmentId, projectId: ProjectId | null) {
  return useEnvironmentQuery(
    projectId === null ? null : featureMapRead({ environmentId, input: { projectId } }),
  );
}
