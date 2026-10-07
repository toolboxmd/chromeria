import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { type EnvironmentId, ThreadId } from "@t3tools/contracts";
import { Link } from "@tanstack/react-router";
import { BotIcon, ChevronRightIcon, MessageSquareIcon } from "lucide-react";
import { useMemo, useState, type ReactNode } from "react";

import { cn } from "~/lib/utils";
import { useThreadShell } from "~/state/entities";
import type { AgentMessage } from "@t3tools/client-runtime/agent-message";

const stopToggle = (event: { stopPropagation: () => void }) => event.stopPropagation();

/**
 * A report or message from another agent's thread: one collapsed line with
 * the sender and a preview, expanding to the full body. The sender's title
 * opens its thread while the thread still exists.
 */
export function AgentMessageRow(props: {
  message: AgentMessage;
  environmentId: EnvironmentId;
  renderBody: (text: string) => ReactNode;
}) {
  const { message, environmentId } = props;
  const [expanded, setExpanded] = useState(false);
  const threadRef = useMemo(
    () => scopeThreadRef(environmentId, ThreadId.make(message.threadId)),
    [environmentId, message.threadId],
  );
  const threadExists = useThreadShell(threadRef) !== null;
  const Icon = message.kind === "report" ? BotIcon : MessageSquareIcon;
  const label = message.kind === "report" ? "Report from" : "Message from";
  const toggle = () => setExpanded((value) => !value);

  return (
    <div className="flex flex-col">
      <div
        role="button"
        tabIndex={0}
        aria-expanded={expanded}
        onClick={toggle}
        onKeyDown={(event) => {
          if (event.target !== event.currentTarget) return;
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            toggle();
          }
        }}
        className="flex min-h-6 cursor-pointer select-none items-center gap-1.5 rounded-md ps-0.5 pe-2 text-sm leading-relaxed transition-colors hover:bg-accent/20 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring/70"
      >
        <span className="flex size-6 shrink-0 items-center justify-center text-icon-muted">
          <Icon aria-hidden className="block size-4 shrink-0 stroke-2" />
        </span>
        <span className="shrink-0 text-secondary-label">{label}</span>
        {threadExists ? (
          <Link
            to="/$environmentId/$threadId"
            params={{ environmentId, threadId: threadRef.threadId }}
            onClick={stopToggle}
            onKeyDown={stopToggle}
            className="min-w-0 max-w-[40%] shrink-0 truncate font-medium text-foreground hover:underline"
          >
            {message.title}
          </Link>
        ) : (
          <span className="min-w-0 max-w-[40%] shrink-0 truncate text-secondary-label">
            {message.title}
          </span>
        )}
        <span
          className={cn("min-w-0 flex-1 truncate text-muted-foreground", expanded && "invisible")}
        >
          {message.preview}
        </span>
        <span className="flex size-4 shrink-0 items-center justify-center" aria-hidden>
          <ChevronRightIcon
            className={cn(
              "size-3 shrink-0 text-icon-muted opacity-70 transition-transform duration-200",
              expanded && "rotate-90",
            )}
          />
        </span>
      </div>
      {expanded ? (
        <div className="ms-7 px-0.5 py-1 select-text">{props.renderBody(message.body)}</div>
      ) : null}
    </div>
  );
}
