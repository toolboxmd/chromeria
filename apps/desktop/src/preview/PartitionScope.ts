import { DEFAULT_BROWSER_PROFILE_ID, INCOGNITO_BROWSER_PROFILE_ID } from "@t3tools/contracts";

/**
 * The default profile keeps the bare environment id it used before profiles
 * existed, so upgrading does not strand anyone's existing logins in an orphaned
 * partition.
 */
export function resolvePartitionScope(
  environmentId: string,
  profileId: string | undefined,
): {
  readonly scope: string;
  readonly persistent: boolean;
  readonly namespace?: "profile";
} {
  if (profileId === undefined || profileId === DEFAULT_BROWSER_PROFILE_ID) {
    return { scope: environmentId, persistent: true };
  }
  // JSON's tuple framing is injective for strings, including lone UTF-16
  // surrogates (which it escapes). URI encoding throws on those supported ids,
  // while replacing them with U+FFFD would collapse distinct identities.
  return {
    scope: JSON.stringify([environmentId, profileId]),
    persistent: profileId !== INCOGNITO_BROWSER_PROFILE_ID,
    namespace: "profile" as const,
  };
}
