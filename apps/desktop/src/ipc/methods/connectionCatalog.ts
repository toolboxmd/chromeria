import { AuthRenewalLocalRecord } from "@t3tools/contracts";
import * as DesktopRenewalStore from "../../app/DesktopRenewalStore.ts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as DesktopConnectionCatalogStore from "../../app/DesktopConnectionCatalogStore.ts";
import * as IpcChannels from "../channels.ts";
import * as DesktopIpc from "../DesktopIpc.ts";

export const getConnectionCatalog = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.GET_CONNECTION_CATALOG_CHANNEL,
  payload: Schema.Void,
  result: Schema.NullOr(Schema.String),
  handler: Effect.fn("desktop.ipc.connectionCatalog.get")(function* () {
    const store = yield* DesktopConnectionCatalogStore.DesktopConnectionCatalogStore;
    return Option.getOrNull(yield* store.get);
  }),
});

export const setConnectionCatalog = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.SET_CONNECTION_CATALOG_CHANNEL,
  payload: Schema.String,
  result: Schema.Boolean,
  handler: Effect.fn("desktop.ipc.connectionCatalog.set")(function* (catalog) {
    const store = yield* DesktopConnectionCatalogStore.DesktopConnectionCatalogStore;
    return yield* store.set(catalog);
  }),
});

export const getRenewalRecord = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.GET_RENEWAL_RECORD_CHANNEL,
  payload: Schema.String,
  result: Schema.NullOr(AuthRenewalLocalRecord),
  handler: Effect.fn("desktop.ipc.renewal.get")(function* (key) {
    return yield* (yield* DesktopRenewalStore.DesktopRenewalStore).read(key);
  }),
});
export const compareRenewalRecord = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.COMPARE_RENEWAL_RECORD_CHANNEL,
  payload: Schema.Struct({
    key: Schema.String,
    revision: Schema.NullOr(Schema.Int),
    value: AuthRenewalLocalRecord,
  }),
  result: Schema.Boolean,
  handler: Effect.fn("desktop.ipc.renewal.compare")(function* (input) {
    return yield* (yield* DesktopRenewalStore.DesktopRenewalStore).compareAndSet(input);
  }),
});
