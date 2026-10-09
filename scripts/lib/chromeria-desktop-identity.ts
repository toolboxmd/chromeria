// Chromeria (toolboxmd fork): the desktop identity a build embeds. The packaged
// app, its Electron profile, URL scheme and default T3 home all come from this
// one definition, selected at build time with CHROMERIA_DESKTOP_VARIANT.
// Dependency-free: the desktop runtime, its vite config and the artifact script share it.

declare const __CHROMERIA_DESKTOP_VARIANT__: string | undefined;

export interface ChromeriaDesktopIdentity {
  readonly variant: "default" | "v2";
  readonly productName: string;
  readonly appId: string;
  readonly artifactPrefix: string;
  /**
   * A fixed Electron profile in every mode that never imports another app's
   * profile. Null keeps upstream's per-mode profile and its V1 imports.
   */
  readonly userDataProfile: string | null;
  /** The only URL scheme the app registers. Null keeps upstream's t3code and t3code-dev. */
  readonly scheme: string | null;
  /** Default T3 home below the user's home directory; T3CODE_HOME still wins. */
  readonly homeDirName: string;
}

export const CHROMERIA_DESKTOP_IDENTITIES = {
  default: {
    variant: "default",
    productName: "Chromeria",
    appId: "md.toolbox.chromeria",
    artifactPrefix: "Chromeria",
    userDataProfile: null,
    scheme: null,
    homeDirName: ".t3",
  },
  // Installs and runs beside the default app, so it shares none of its identity.
  v2: {
    variant: "v2",
    productName: "Chromeria V2",
    appId: "md.toolbox.chromeria.v2",
    artifactPrefix: "Chromeria-V2",
    userDataProfile: "chromeria-v2",
    scheme: "chromeria-v2",
    homeDirName: ".t3-v2",
  },
} as const satisfies Record<string, ChromeriaDesktopIdentity>;

/** Unset or empty selects the default app; any unknown value is a build error. */
export function resolveChromeriaDesktopIdentity(
  variant: string | undefined,
): ChromeriaDesktopIdentity {
  const value = variant?.trim() || "default";
  if (value === "default" || value === "v2") return CHROMERIA_DESKTOP_IDENTITIES[value];
  throw new Error(`Unknown CHROMERIA_DESKTOP_VARIANT "${value}". Use "v2" or leave it unset.`);
}

/** The identity this bundle was built with. Tests and unbundled runs get the default. */
export const CHROMERIA_DESKTOP_IDENTITY = resolveChromeriaDesktopIdentity(
  typeof __CHROMERIA_DESKTOP_VARIANT__ === "undefined" ? undefined : __CHROMERIA_DESKTOP_VARIANT__,
);
