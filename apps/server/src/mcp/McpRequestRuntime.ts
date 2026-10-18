import * as NativeReceipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Layer from "effect/Layer";
import * as Receipts from "./McpRequestReceipts.ts";
import * as Forwarding from "../peer/PeerForwarding.ts";
import * as Attachments from "../assets/AttachmentStaging.ts";

/** The server and production-composition proof share these exact request owners. */
export const layer = Layer.mergeAll(
  Receipts.layer,
  Attachments.layer.pipe(Layer.provide(NativeReceipts.layer)),
);

/** Keep the source forwarder and transports on the same durable request owners. */
export const layerForwarding = Forwarding.layer.pipe(Layer.provideMerge(layer));
