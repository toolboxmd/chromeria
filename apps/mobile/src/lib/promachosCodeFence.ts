import {
  flattenThemeColor,
  type MobileThemeAppearance,
  type MobileThemeVariables,
} from "./mobileTheme";

/** A dark Promachos reply's fence is opaque over the screen, so its lighter bubble cannot show through. */
export function promachosReplyCodeFence(
  theme: Pick<MobileThemeVariables, "--color-md-code-bg" | "--color-screen">,
  appearance: MobileThemeAppearance,
): string {
  return appearance === "dark"
    ? flattenThemeColor(theme["--color-md-code-bg"], theme["--color-screen"])
    : theme["--color-md-code-bg"];
}
