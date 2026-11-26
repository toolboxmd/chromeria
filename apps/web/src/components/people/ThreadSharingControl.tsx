import { useAtomValue } from "@effect/atom-react";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  AuthOrchestrationOperateScope,
  CommandId,
  type EnvironmentId,
  type ThreadId,
} from "@t3tools/contracts";
import { msg, T, useGT, useMessages } from "gt-react";
import { UsersIcon } from "lucide-react";

import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";

import { m, type MessageResolver } from "../../i18n/messages";
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

function actionLabel(action: ThreadSharingAction, resolve: MessageResolver = m): string {
  switch (action.type) {
    case "thread.share":
      return resolve(msg("Share with {name}", { name: action.coOwner }));
    case "thread.unshare":
      return resolve(msg("Unshare", { $context: "verb, stops sharing a thread" }));
    case "thread.leave":
      return resolve(msg("Leave", { $context: "verb, leaves a shared thread" }));
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
  const gt = useGT();
  const messages = useMessages();
  const thread = useThreadShell(scopeThreadRef(environmentId, threadId));
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
        <T context="thread sharing state">Shared</T>
      </Badge>
    ) : null;
  }
  const sharedWith = gt("Shared with {names}", { names: coOwners.join(", ") });

  return (
    <Menu>
      <MenuTrigger
        render={
          <Button
            size="xs"
            variant={shared ? "outline" : "ghost-muted"}
            aria-label={shared ? sharedWith : gt("Share thread")}
            title={shared ? sharedWith : gt("Share thread")}
          />
        }
      >
        <UsersIcon />
        {shared ? (
          <span>
            <T context="thread sharing state">Shared</T>
          </span>
        ) : null}
      </MenuTrigger>
      <MenuPopup align="end">
        <MenuItem
          onClick={() => {
            if (!readEnvironmentScope(environmentId, AuthOrchestrationOperateScope)) return;
            void runtime
              .runPromise(
                SecureRandom.SecureRandom.pipe(
                  Effect.flatMap((crypto) => crypto.uuidV4()),
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
          {actionLabel(action, messages)}
        </MenuItem>
      </MenuPopup>
    </Menu>
  );
}
