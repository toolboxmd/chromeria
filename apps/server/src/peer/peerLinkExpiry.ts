import * as DateTime from "effect/DateTime";

import { REUSABLE_DEV_SESSION_EXPIRES_AT } from "../auth/ReusableDevAuth.ts";

/** A peer's relative lifetime must not move the permanent-session ceiling when clocks differ. */
export const peerLinkExpiry = (now: DateTime.Utc, expiresIn: number) =>
  DateTime.makeUnsafe(
    Math.min(
      now.epochMilliseconds + expiresIn * 1_000,
      REUSABLE_DEV_SESSION_EXPIRES_AT.epochMilliseconds,
    ),
  );
