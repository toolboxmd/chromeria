// @effect-diagnostics nodeBuiltinImport:off - The desktop pack writes this marker from a bundler hook, outside an Effect runtime.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

// Chromeria (toolboxmd fork): `--skip-build` packages whatever dist-electron
// holds. main.cjs is the only desktop bundle that embeds the identity (preload
// reads branding over IPC), so the marker binds its exact bytes to the variant
// it was built for, and packaging refuses any other pairing.
export const DESKTOP_IDENTITY_MARKER = "chromeria-desktop-identity.json";
const MAIN_BUNDLE = "main.cjs";

const sha256 = (file: string) =>
  NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(file)).digest("hex");

export function writeDesktopIdentityMarker(distDir: string, variant: string): void {
  const mainSha256 = sha256(NodePath.join(distDir, MAIN_BUNDLE));
  NodeFS.writeFileSync(
    NodePath.join(distDir, DESKTOP_IDENTITY_MARKER),
    `${JSON.stringify({ variant, mainSha256 })}\n`,
  );
}

/** For the main.ts pack only: runs after main.cjs is written, so a failed pack leaves no marker. */
export function desktopIdentityMarkerPlugin(variant: string) {
  return {
    name: "chromeria-desktop-identity-marker",
    writeBundle(options: { readonly dir?: string | undefined }) {
      if (!options.dir) throw new Error("The desktop identity marker needs an output directory.");
      writeDesktopIdentityMarker(options.dir, variant);
    },
  };
}

/** Why dist-electron cannot be packaged as `variant`, or undefined when it can. */
export function desktopIdentityMarkerMismatch(
  distDir: string,
  variant: string,
): string | undefined {
  let marker: { readonly variant?: unknown; readonly mainSha256?: unknown };
  try {
    marker = JSON.parse(
      NodeFS.readFileSync(NodePath.join(distDir, DESKTOP_IDENTITY_MARKER), "utf8"),
    );
  } catch {
    return `${DESKTOP_IDENTITY_MARKER} is missing or unreadable`;
  }
  if (marker.variant !== variant) {
    return `${MAIN_BUNDLE} was built for variant "${String(marker.variant)}", not "${variant}"`;
  }
  let mainSha256: string;
  try {
    mainSha256 = sha256(NodePath.join(distDir, MAIN_BUNDLE));
  } catch {
    return `${MAIN_BUNDLE} is missing`;
  }
  return marker.mainSha256 === mainSha256
    ? undefined
    : `${MAIN_BUNDLE} does not match its identity marker`;
}
