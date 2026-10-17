import { createPeerGrantState } from "@t3tools/client-runtime/state/peer-grants";
import { connectionAtomRuntime } from "../connection/runtime";
const grants = createPeerGrantState(connectionAtomRuntime);
export const peerGrants = grants.list;
export const updatePeerGrant = grants.update;
