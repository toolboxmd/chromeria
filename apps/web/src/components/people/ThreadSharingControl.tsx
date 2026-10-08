import { useAtomValue } from "@effect/atom-react";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  AuthOrchestrationOperateScope,
  CommandId,
  type EnvironmentId,
  type ThreadId,
} from "@t3tools/contracts";
import { UsersIcon } from "lucide-react";
import { useMemo } from "react";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";

import { runtime } from "../../lib/runtime";
import { readEnvironmentScope, useEnvironmentScope } from "../../state/session";
import { useThreadShell } from "../../state/entities";
import { useAtomCommand } from "../../state/use-atom-command";
import { Badge } from "../ui/badge";
import { Button } from "../ui/button";
import { Menu, MenuItem, MenuPopup, MenuTrigger } from "../ui/menu";
import { threadSharingAction, type ThreadSharingAction } from "./personView";
import { threadSharingCommand } from "./threadSharing";
import { useEnvironmentPerson, useRefreshDevicePeopleOnFocus } from "./usePersonView";

function actionLabel(action: ThreadSharingAction): string {
  switch (action.type) {
    case "thread.share":
      return `Share with ${action.coOwner}`;
    case "thread.unshare":
      return "Unshare";
    case "thread.leave":
      return "Leave";
  }
}

/**
 * The thread header's Shared label and its sharing action: the owner shares
 * or unshares, a co-owner leaves.
 */
export function ThreadSharingControl({
  environmentId,
  threadId,
}: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}) {
  const thread = useThreadShell(
    useMemo(() => scopeThreadRef(environmentId, threadId), [environmentId, threadId]),
  );
  const person = useEnvironmentPerson(environmentId);
  useRefreshDevicePeopleOnFocus();
  const dispatch = useAtomCommand(threadSharingCommand);
  const canDispatch = useAtomValue(threadSharingCommand.permissionAtom(environmentId));
  const canOperate = useEnvironmentScope(environmentId, AuthOrchestrationOperateScope);
  if (thread === null) return null;

  const coOwners = thread.source.coOwners ?? [];
  const shared = coOwners.length > 0;
  const action = canOperate && canDispatch ? threadSharingAction(thread.source, person) : null;
  if (action === null) {
    return shared ? (
      <Badge size="sm" variant="secondary">
        Shared
      </Badge>
    ) : null;
  }
  const sharedWith = `Shared with ${coOwners.join(", ")}`;

  return (
    <Menu>
      <MenuTrigger
        render={
          <Button
            size="xs"
            variant={shared ? "outline" : "ghost-muted"}
            aria-label={shared ? sharedWith : "Share thread"}
            title={shared ? sharedWith : "Share thread"}
          />
        }
      >
        <UsersIcon />
        {shared ? <span>Shared</span> : null}
      </MenuTrigger>
      <MenuPopup align="end">
        <MenuItem
          onClick={() => {
            if (!readEnvironmentScope(environmentId, AuthOrchestrationOperateScope)) return;
            void runtime
              .runPromise(
                Crypto.Crypto.pipe(
                  Effect.flatMap((crypto) => crypto.randomUUIDv4),
                  Effect.orDie,
                ),
              )
              .then((id) =>
                dispatch({
                  environmentId,
                  input: { ...action, threadId, actor: person, commandId: CommandId.make(id) },
                }),
              );
          }}
        >
          {actionLabel(action)}
        </MenuItem>
      </MenuPopup>
    </Menu>
  );
}
