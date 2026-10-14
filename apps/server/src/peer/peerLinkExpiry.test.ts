import { expect, it } from "@effect/vitest";
import { NodeHttpServer } from "@effect/platform-node";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as PeerLinks from "./PeerLinks.ts";
import { descriptorOf, layerLinkingEnvironment, servePeer } from "./PeerLinks.testkit.ts";
import { peerLinkExpiry } from "./peerLinkExpiry.ts";

it.effect("stores the capped expiry after linking to a peer whose clock is behind", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* servePeer(descriptorOf("expiry-target", "Target"), Layer.empty);
      const context = yield* Layer.build(
        layerLinkingEnvironment(descriptorOf("expiry-source", "Source")),
      );
      const links = Context.get(context, PeerLinks.PeerLinks);
      const clock = yield* Clock.Clock;
      const sourceNow = (yield* Clock.currentTimeMillis) + 1_050;
      const pairing = yield* b.auth.issuePairingCredential();
      const linked = yield* links
        .link({ url: b.url, pairingCode: pairing.credential, access: "auto" })
        .pipe(
          Effect.provideService(Clock.Clock, {
            ...clock,
            currentTimeMillisUnsafe: () => sourceNow,
            currentTimeMillis: Effect.succeed(sourceNow),
          }),
        );
      expect(DateTime.formatIso(linked.expiresAt)).toBe("9999-12-31T23:59:59.999Z");
      const stored = yield* links.get(linked.environmentId);
      expect(DateTime.formatIso(Option.getOrThrow(stored).expiresAt)).toBe(
        "9999-12-31T23:59:59.999Z",
      );
      const [listed] = yield* links.list;
      expect(DateTime.formatIso(listed!.expiresAt)).toBe("9999-12-31T23:59:59.999Z");
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it("caps permanent links when the source clock is ahead of the target", () => {
  const targetNow = Date.parse("2026-10-14T12:00:00.000Z");
  const expiresIn = Math.floor((Date.parse("9999-12-31T23:59:59.999Z") - targetNow) / 1_000);
  const sourceNow = DateTime.makeUnsafe("2026-10-14T12:00:01.050Z");

  expect(DateTime.formatIso(peerLinkExpiry(sourceNow, expiresIn))).toBe("9999-12-31T23:59:59.999Z");
});

it("preserves a finite lifetime and a source clock behind the target", () => {
  const now = DateTime.makeUnsafe("2026-10-14T12:00:00.000Z");
  expect(DateTime.formatIso(peerLinkExpiry(now, 60))).toBe("2026-10-14T12:01:00.000Z");
  const targetNow = Date.parse("2026-10-14T12:00:01.000Z");
  const expiresIn = Math.floor((Date.parse("9999-12-31T23:59:59.999Z") - targetNow) / 1_000);
  expect(DateTime.formatIso(peerLinkExpiry(now, expiresIn))).toBe("9999-12-31T23:59:58.000Z");
});

it("caps before constructing a date even if the sum exceeds the JavaScript date range", () => {
  const now = DateTime.makeUnsafe(8_640_000_000_000_000);
  expect(DateTime.formatIso(peerLinkExpiry(now, 60))).toBe("9999-12-31T23:59:59.999Z");
});
