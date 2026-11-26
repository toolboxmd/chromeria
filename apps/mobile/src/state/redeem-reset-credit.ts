import { createRedeemResetCreditCommand } from "@t3tools/client-runtime/state/redeemed-reset";

import { connectionAtomRuntime } from "../connection/runtime";
import { environmentPresentations } from "./presentation";
import { serverEnvironment } from "./server";

/** Redeems a reset credit, then clears the old block on every other holder of the same login. */
export const redeemResetCredit = createRedeemResetCreditCommand(connectionAtomRuntime, {
  consume: serverEnvironment.consumeResetCredit,
  environmentsAtom: environmentPresentations.presentationsWithProvidersAtom,
});
