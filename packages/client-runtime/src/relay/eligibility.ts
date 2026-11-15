import type { RelayClientEnvironmentRecord } from "@t3tools/contracts/relay";

export function isAddableRelayEnvironment(environment: RelayClientEnvironmentRecord | undefined) {
  // Account discovery also includes publishing links that EnvironmentConnector rejects.
  return environment?.endpoint.providerKind === "cloudflare_tunnel";
}
