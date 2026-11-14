import type { ExpoConfig } from "expo/config";

import { BRAND_ASSET_PATHS } from "../../scripts/lib/brand-assets.ts";

/**
 * Chromeria mobile identity for app.config.ts: `CHROMERIA_MOBILE=1` builds the production variant
 * under the fork's Apple team with OTA updates off, so upstream's EAS bundles never load; each
 * TestFlight upload needs a higher `CHROMERIA_IOS_BUILD_NUMBER`. The `t3code` URL scheme stays.
 */
export function resolveChromeriaMobileIdentity(env: Readonly<Record<string, string | undefined>>) {
  if (env.CHROMERIA_MOBILE !== "1") return null;

  const buildNumber = env.CHROMERIA_IOS_BUILD_NUMBER?.trim();
  if (buildNumber && !/^0*[1-9]\d*$/.test(buildNumber)) {
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
      const { eas: _upstreamEasProject, ...extra } = config.extra ?? {};
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
