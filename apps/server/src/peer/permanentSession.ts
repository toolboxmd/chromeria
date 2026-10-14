import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import { REUSABLE_DEV_SESSION_EXPIRES_AT } from "../auth/ReusableDevAuth.ts";

/** Only server-approved pairing-code peer links receive the permanent TTL. */
export const permanentPeerSessionTtl = Clock.currentTimeMillis.pipe(
  Effect.map((now) => Duration.millis(REUSABLE_DEV_SESSION_EXPIRES_AT.epochMilliseconds - now)),
);
