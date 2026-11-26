import { useGT } from "gt-react";

import { Badge } from "../ui/badge";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";

/** Fork (toolboxmd/chromeria#319): marks a thread that started as a bare launch. */
export function ThreadBareBadge() {
  const gt = useGT();
  return (
    <Tooltip>
      <TooltipTrigger render={<Badge variant="secondary" size="sm" />}>{gt("Bare")}</TooltipTrigger>
      <TooltipPopup side="top" className="max-w-64">
        {gt(
          "Started bare, without instructions, plugins, MCP servers or skills. The timeline notice lists what was removed.",
        )}
      </TooltipPopup>
    </Tooltip>
  );
}
