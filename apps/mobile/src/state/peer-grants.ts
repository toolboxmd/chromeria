import { createPeerGrantState } from "@t3tools/client-runtime/state/peer-grants";
import { connectionAtomRuntime } from "../connection/runtime";
export const peerGrants = createPeerGrantState(connectionAtomRuntime);
