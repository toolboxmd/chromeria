import {
  IssueRpcs,
  DeviceViewerClientRpcGroup,
  PreviewViewerClientRpcGroup,
  FeatureMapRpcs,
  PromachosHomeRpcGroup,
  IssueLinksRpcGroup,
  ScheduledTaskRunsRpcGroup,
  SpectrumRpcGroup,
  RpcScopeAuthorization,
} from "@t3tools/contracts";
import * as RpcGroup from "effect/rpc/RpcGroup";
import { RpcInstrumentation } from "./observability/RpcInstrumentation.ts";

export const group = RpcGroup.make(...IssueRpcs, ...FeatureMapRpcs)
  .merge(
    PromachosHomeRpcGroup,
    IssueLinksRpcGroup,
    ScheduledTaskRunsRpcGroup,
    SpectrumRpcGroup,
    DeviceViewerClientRpcGroup,
    PreviewViewerClientRpcGroup,
  )
  .middleware(RpcScopeAuthorization)
  .middleware(RpcInstrumentation);

export const methods = Array.from(group.requests.values(), (rpc) => rpc._tag);
