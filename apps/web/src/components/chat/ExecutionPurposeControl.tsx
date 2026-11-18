import { T, useGT, Var } from "gt-react";
import { useState } from "react";
import {
  type ExecutionPurposeScope,
  type OrchestrationV2Run,
  executionPurposeOf,
  parseTrackingReferenceUrl,
  purposeScopeLabel,
  purposeScopesEqual,
  trackingReferenceUrl,
} from "@t3tools/contracts";
import { Button } from "../ui/button";
import { Input } from "../ui/input";

export function ExecutionPurposeControl(props: {
  readonly scope: ExecutionPurposeScope;
  readonly runs: ReadonlyArray<Pick<OrchestrationV2Run, "id" | "ordinal" | "purpose" | "status">>;
  readonly disabled: boolean;
  readonly onChange: (scope: ExecutionPurposeScope) => Promise<boolean>;
}) {
  const gt = useGT();
  const [expanded, setExpanded] = useState(false);
  const [url, setUrl] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const active = props.runs.findLast((run) =>
    ["preparing", "starting", "running", "waiting"].includes(run.status),
  );
  const save = async (scope: ExecutionPurposeScope) => {
    setSaving(true);
    const saved = await props.onChange(scope).catch(() => false);
    setSaving(false);
    if (saved) {
      setExpanded(false);
      setError(null);
      setUrl("");
    } else setError(gt("The purpose could not be saved. Refresh the selection and try again."));
  };
  return (
    <div className="px-3 py-1 text-xs text-muted-foreground">
      <Button
        variant="ghost"
        size="sm"
        disabled={props.disabled || saving}
        onClick={() => setExpanded(!expanded)}
      >
        <T>
          Purpose: <Var>{purposeScopeLabel(props.scope)}</Var>
        </T>
      </Button>
      {active !== undefined &&
      !purposeScopesEqual(executionPurposeOf(active).scope, props.scope) ? (
        <p>
          <T>
            Running: <Var>{purposeScopeLabel(executionPurposeOf(active).scope)}</Var>. Send queues
            the selected purpose; Steer keeps the running purpose.
          </T>
        </p>
      ) : null}
      {expanded ? (
        <div className="flex flex-col gap-2 py-2">
          <label>
            <T>Issue or PR URL</T>
            <Input
              value={url}
              onChange={(event) => setUrl(event.target.value)}
              placeholder="https://github.com/owner/repo/issues/123"
              disabled={props.disabled || saving}
            />
          </label>
          <div className="flex gap-2">
            <Button
              size="sm"
              disabled={props.disabled || saving}
              onClick={() => {
                const reference = parseTrackingReferenceUrl(url);
                if (reference === null) {
                  setError(gt("Enter a valid Issue or PR URL."));
                  return;
                }
                void save({ type: "single", reference });
              }}
            >
              <T>Select purpose</T>
            </Button>
            <Button
              variant="outline"
              size="sm"
              disabled={props.disabled || saving}
              onClick={() => void save({ type: "unscoped", reason: "deliberate" })}
            >
              <T>Clear purpose</T>
            </Button>
          </div>
          {error ? <p role="alert">{error}</p> : null}
          {props.runs.length > 0 ? (
            <ol className="max-h-48 space-y-1 overflow-y-auto">
              {props.runs.map((run) => {
                const purpose = executionPurposeOf(run);
                return (
                  <li key={run.id}>
                    <T>
                      Run <Var>{run.ordinal}</Var>:{" "}
                      <Var>
                        {purpose.scope.type === "single" ? (
                          <a
                            href={trackingReferenceUrl(purpose.scope.reference)}
                            target="_blank"
                            rel="noreferrer"
                          >
                            {purposeScopeLabel(purpose.scope)}
                          </a>
                        ) : (
                          purposeScopeLabel(purpose.scope)
                        )}
                      </Var>{" "}
                      (<Var>{purpose.provenance.type.replaceAll("_", " ")}</Var>)
                    </T>
                  </li>
                );
              })}
            </ol>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
