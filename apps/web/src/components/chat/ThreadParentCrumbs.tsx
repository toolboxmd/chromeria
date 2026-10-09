// Fork: header crumbs for a child thread (toolboxmd/chromeria#201, ported from v1 #18).
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { Link, useNavigate } from "@tanstack/react-router";
import { ChevronsUpDownIcon } from "lucide-react";
import { useMemo } from "react";

import { useThreadShell, useThreadShells } from "~/state/entities";
import { Button } from "../ui/button";
import { Menu, MenuItem, MenuItemLabel, MenuPopup, MenuTrigger } from "../ui/menu";
import {
  WorkspaceBreadcrumbItem,
  WorkspaceBreadcrumbSeparator,
  WorkspaceBreadcrumbText,
} from "../WorkspaceBreadcrumb";
import { childParentThreadId, siblingThreads } from "./threadParentCrumbs.logic";

/**
 * The parent title links back to the parent, and a compact menu switches
 * between siblings. Renders nothing for threads the user started.
 */
export function ThreadParentCrumbs({
  environmentId,
  threadId,
}: {
  environmentId: EnvironmentId;
  threadId: ThreadId;
}) {
  const self = useThreadShell(
    useMemo(() => scopeThreadRef(environmentId, threadId), [environmentId, threadId]),
  );
  const parentId = childParentThreadId(self);
  const parent = useThreadShell(
    useMemo(
      () => (parentId === null ? null : scopeThreadRef(environmentId, parentId)),
      [environmentId, parentId],
    ),
  );
  // Only child threads subscribe to the full shell list.
  const shells = useThreadShells(parent !== null);
  const navigate = useNavigate();
  const siblings = useMemo(
    () => (parentId === null ? [] : siblingThreads(shells, environmentId, parentId, threadId)),
    [environmentId, parentId, shells, threadId],
  );
  if (parent === null) return null;
  return (
    <>
      <WorkspaceBreadcrumbItem className="shrink">
        <Link
          to="/$environmentId/$threadId"
          params={{ environmentId, threadId: parent.id }}
          aria-label={`Open parent thread ${parent.title}`}
          className="inline-flex min-w-0 max-w-full items-center rounded-sm text-muted-foreground transition-colors hover:text-foreground focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring"
        >
          <WorkspaceBreadcrumbText className="max-w-48">{parent.title}</WorkspaceBreadcrumbText>
        </Link>
        {siblings.length > 0 ? (
          <Menu>
            <MenuTrigger
              render={
                <Button
                  size="icon-xs"
                  variant="ghost"
                  aria-label={`Switch to one of ${siblings.length} sibling threads`}
                />
              }
            >
              <ChevronsUpDownIcon className="size-3" />
            </MenuTrigger>
            <MenuPopup align="start" aria-label="Sibling threads">
              {siblings.map((sibling) => (
                <MenuItem
                  key={sibling.id}
                  onClick={() =>
                    void navigate({
                      to: "/$environmentId/$threadId",
                      params: { environmentId, threadId: sibling.id },
                    })
                  }
                >
                  <MenuItemLabel>{sibling.title}</MenuItemLabel>
                </MenuItem>
              ))}
            </MenuPopup>
          </Menu>
        ) : null}
      </WorkspaceBreadcrumbItem>
      <WorkspaceBreadcrumbSeparator>
        <WorkspaceBreadcrumbText>/</WorkspaceBreadcrumbText>
      </WorkspaceBreadcrumbSeparator>
    </>
  );
}
