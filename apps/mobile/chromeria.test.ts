import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { resolveChromeriaMobileIdentity } from "./chromeria.ts";

async function resolveConfig(env: Record<string, string | undefined>) {
  vi.resetModules();
  for (const [key, value] of Object.entries(env)) vi.stubEnv(key, value);
  vi.doMock("../../scripts/lib/public-config.ts", () => ({ loadRepoEnv: () => env }));
  return (await import("./app.config.ts")).default;
}

afterEach(() => {
  vi.unstubAllEnvs();
  vi.doUnmock("../../scripts/lib/public-config.ts");
});

describe("Chromeria mobile identity", () => {
  it.each([undefined, "0", "true"])("requires the explicit switch (%s)", (flag) => {
    expect(
      resolveChromeriaMobileIdentity({
        CHROMERIA_MOBILE: flag,
        CHROMERIA_IOS_BUILD_NUMBER: "invalid",
      }),
    ).toBeNull();
  });

  it.each(["0", "000", "-1", "1.2", "4x", "1e2"])(
    "rejects invalid upload build number %s",
    (buildNumber) => {
      expect(() =>
        resolveChromeriaMobileIdentity({
          CHROMERIA_MOBILE: "1",
          CHROMERIA_IOS_BUILD_NUMBER: buildNumber,
        }),
      ).toThrow("CHROMERIA_IOS_BUILD_NUMBER must be a positive integer.");
    },
  );

  it.each([undefined, "", "  "])("allows an unallocated build number (%s)", (buildNumber) => {
    const identity = resolveChromeriaMobileIdentity({
      CHROMERIA_MOBILE: "1",
      CHROMERIA_IOS_BUILD_NUMBER: buildNumber,
    });
    expect(identity?.apply({ name: "Chromeria", slug: "t3-code" }).ios).not.toHaveProperty(
      "buildNumber",
    );
  });

  it("resolves production identity, extension identifiers and isolated OTA configuration", async () => {
    const config = await resolveConfig({
      APP_VARIANT: "production",
      CHROMERIA_MOBILE: "1",
      CHROMERIA_IOS_BUILD_NUMBER: " 4 ",
    });
    expect(config).toMatchObject({
      name: "Chromeria",
      scheme: "t3code",
      icon: "../../assets/chromeria/chromeria-ios-1024.png",
      updates: { enabled: false },
      ios: {
        bundleIdentifier: "md.toolbox.chromeria.mobile",
        appleTeamId: "MCBD97AVW6",
        buildNumber: "4",
        icon: "../../assets/chromeria/chromeria-ios-1024.png",
        entitlements: {
          "keychain-access-groups": ["$(AppIdentifierPrefix)md.toolbox.chromeria.mobile"],
        },
      },
      android: { package: "md.toolbox.chromeria.mobile" },
      extra: {
        clerk: { publishableKey: null, jwtTemplate: null },
        relay: { url: null },
      },
    });
    expect(config.updates).toEqual({ enabled: false });
    expect(config).not.toHaveProperty("owner");
    expect(config.extra).not.toHaveProperty("eas");
    expect(config.plugins).toContainEqual([
      "expo-widgets",
      expect.objectContaining({
        bundleIdentifier: "md.toolbox.chromeria.mobile.widgets",
        groupIdentifier: "group.md.toolbox.chromeria.mobile",
      }),
    ]);
    expect(config.plugins).toContainEqual([
      "expo-sharing",
      expect.objectContaining({
        ios: expect.objectContaining({
          enabled: true,
          extensionBundleIdentifier: "md.toolbox.chromeria.mobile.sharing",
          appGroupId: "group.md.toolbox.chromeria.mobile",
        }),
      }),
    ]);
    expect(config.plugins).toContainEqual([
      "expo-splash-screen",
      expect.objectContaining({ image: "../../assets/chromeria/chromeria-icon-1024.png" }),
    ]);
  });

  it("preserves optional Connect configuration from the final PR209 decision", async () => {
    const config = await resolveConfig({
      CHROMERIA_MOBILE: "1",
      EXPO_PUBLIC_CLERK_PUBLISHABLE_KEY: "pk_test_fixture",
      EXPO_PUBLIC_CLERK_JWT_TEMPLATE: "fixture-template",
      EXPO_PUBLIC_CLERK_GOOGLE_IOS_URL_SCHEME: "fixture-oauth",
      T3CODE_RELAY_URL: "https://relay.example.test",
    });
    expect(config.extra).toMatchObject({
      clerk: { publishableKey: "pk_test_fixture", jwtTemplate: "fixture-template" },
      relay: { url: "https://relay.example.test" },
      EXPO_PUBLIC_CLERK_GOOGLE_IOS_URL_SCHEME: "fixture-oauth",
    });
    expect(config.updates).toEqual({ enabled: false });
    expect(config.extra).not.toHaveProperty("eas");
  });

  it("retains the reduced-capability personal-team path", async () => {
    const config = await resolveConfig({
      APP_VARIANT: "production",
      CHROMERIA_MOBILE: "1",
      CHROMERIA_IOS_BUILD_NUMBER: "4",
      T3CODE_IOS_PERSONAL_TEAM: "1",
      T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID: "md.toolbox.chromeria.mobile",
    });
    expect(config.ios).toMatchObject({
      bundleIdentifier: "md.toolbox.chromeria.mobile",
      appleTeamId: "MCBD97AVW6",
      buildNumber: "4",
    });
    expect(config.plugins).toContainEqual([
      "@clerk/expo",
      expect.objectContaining({ appleSignIn: false }),
    ]);
    expect(config.plugins).toContainEqual([
      "expo-sharing",
      expect.objectContaining({ ios: expect.objectContaining({ enabled: false }) }),
    ]);
    expect(config.plugins).not.toContainEqual(["expo-widgets", expect.anything()]);
    expect(config.plugins).toContain("./plugins/withoutIosPersonalTeamCapabilities.cjs");
  });
});
