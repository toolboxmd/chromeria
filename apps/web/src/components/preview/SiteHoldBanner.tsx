import type { PreviewSiteHold } from "@t3tools/contracts";
import { T, Var } from "gt-react";
import { ShieldAlertIcon } from "lucide-react";

import { Alert, AlertAction, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";

/** Agents stopped on this site; the person passes its check in this tab, then says so. */
export function SiteHoldBanner({
  hold,
  onDone,
}: {
  readonly hold: PreviewSiteHold;
  readonly onDone: () => void;
}) {
  const site = hold.site;
  return (
    <Alert variant="warning" className="m-2">
      <ShieldAlertIcon />
      <AlertTitle>
        {hold.kind === "challenge" ? (
          <T>
            <Var>{site}</Var> asks for a human check
          </T>
        ) : (
          <T>
            <Var>{site}</Var> blocked this browser
          </T>
        )}
      </AlertTitle>
      <AlertDescription>
        {hold.kind === "challenge" ? (
          <T>
            Agents stopped acting on this site. Take control, pass the check, release control, then
            tell the agent to continue.
          </T>
        ) : (
          <T>
            Agents stopped acting on this site. Clear the hold when the site works again in this
            browser.
          </T>
        )}
      </AlertDescription>
      <AlertAction>
        <Button variant="default" size="sm" onClick={onDone}>
          {hold.kind === "challenge" ? <T>Done</T> : <T>Clear hold</T>}
        </Button>
      </AlertAction>
    </Alert>
  );
}
