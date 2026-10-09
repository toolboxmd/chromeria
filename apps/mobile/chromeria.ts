import type { ExpoConfig } from "expo/config";

import { BRAND_ASSET_PATHS } from "../../scripts/lib/brand-assets.ts";

/**
 * Chromeria (toolboxmd fork) identity for the mobile app, used by app.config.ts.
 *
 * `CHROMERIA_MOBILE=1` builds the production variant as Chromeria under the
 * fork's Apple team. OTA updates are off so the app never loads upstream's EAS
 * bundles; TestFlight uploads deliver new builds instead, and App Store Connect
 * needs a higher `CHROMERIA_IOS_BUILD_NUMBER` for every upload.
 * The URL scheme stays `t3code`: widget deep links and linking prefixes use it.
 */
export function resolveChromeriaMobileIdentity(env: Readonly<Record<string, string | undefined>>) {
  if (env.CHROMERIA_MOBILE !== "1") return null;

  const buildNumber = env.CHROMERIA_IOS_BUILD_NUMBER?.trim();
  if (buildNumber && !/^\d+$/.test(buildNumber)) {
    throw new Error("CHROMERIA_IOS_BUILD_NUMBER must be a positive integer.");
  }

  return {
    variant: {
      appName: "Chromeria",
      iosBundleIdentifier: "md.toolbox.chromeria.mobile",
      androidPackage: "md.toolbox.chromeria.mobile",
    },
    assets: {
      appIcon: `../../${BRAND_ASSET_PATHS.chromeriaIosIconPng}`,
      iosIcon: `../../${BRAND_ASSET_PATHS.chromeriaIosIconPng}`,
      splashIcon: `../../${BRAND_ASSET_PATHS.chromeriaIconPng}`,
    },
    apply(config: ExpoConfig): ExpoConfig {
      const { owner: _upstreamOwner, ...rest } = config;
      // Upstream's Clerk and relay only trust upstream's app identity, so a
      // Chromeria build never carries them, even when a build machine's env sets them.
      const {
        eas: _upstreamEasProject,
        clerk: _upstreamClerk,
        relay: _upstreamRelay,
        ...extra
      } = config.extra ?? {};
      return {
        ...rest,
        updates: { enabled: false },
        ios: {
          ...config.ios,
          appleTeamId: "MCBD97AVW6",
          ...(buildNumber ? { buildNumber } : {}),
        },
        extra,
      };
    },
  };
}
