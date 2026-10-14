import type { AuthRenewalLocalRecord } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type { ConnectionTransientError } from "./model.ts";

export class RenewalRecordStore extends Context.Service<
  RenewalRecordStore,
  {
    readonly read: (
      key: string,
    ) => Effect.Effect<AuthRenewalLocalRecord | null, ConnectionTransientError>;
    /** The backend atomically compares and writes, across windows and asynchronous awaits. */
    readonly compareAndSet: (
      key: string,
      revision: number | null,
      value: AuthRenewalLocalRecord,
    ) => Effect.Effect<boolean, ConnectionTransientError>;
  }
>()("@t3tools/client-runtime/connection/renewalRecordStore") {}
