import { planPrismKitsReset } from "./PrismSettings.logic";

export function planPromachosReset(
  scope: Parameters<typeof planPrismKitsReset>[0],
  environments: Parameters<typeof planPrismKitsReset>[1],
): ReturnType<typeof planPrismKitsReset> {
  return planPrismKitsReset(scope, environments, ["promachos"]);
}
