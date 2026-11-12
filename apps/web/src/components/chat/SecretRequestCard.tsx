import { pipe } from "effect/Function";
import {
  SECRET_REQUEST_DEFAULT_PLACEHOLDER,
  secretRequestAnswerInput,
  secretRequestComplete,
  secretRequestDisplay,
  secretRequestFailureMessage,
  secretRequestLink,
  secretRequestPrivacyNote,
  secretRequestReference,
  secretRequestSources,
  secretRequestFields,
  type SecretRequestItem,
  type SecretRequestReply,
  type SecretRequestSource,
} from "@t3tools/client-runtime/secret-request";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import type {
  EnvironmentId,
  OrchestrationV2ProjectedTurnItem,
  SecretProviderId,
} from "@t3tools/contracts";
import { useAtomValue } from "@effect/atom-react";
import { CheckIcon, ExternalLinkIcon, LockIcon, MinusIcon, ShieldCheckIcon } from "lucide-react";
import { useId, useRef, useState, type FormEvent } from "react";

import { isSecretsFeatureEnabled } from "../../env";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { SecretSourceSelect } from "../settings/SecretSourceInput";
import { WorkLogRow } from "./WorkLog";

export function SecretRequestCard(props: {
  readonly environmentId: EnvironmentId;
  readonly item: SecretRequestItem;
  readonly visibility: OrchestrationV2ProjectedTurnItem["visibility"];
}) {
  const { item } = props;
  const display = secretRequestDisplay(item, props.visibility);
  if (display.kind === "answered" || display.kind === "pending-elsewhere") {
    const Icon =
      display.kind === "pending-elsewhere"
        ? LockIcon
        : display.outcome === "saved"
          ? CheckIcon
          : MinusIcon;
    return (
      <WorkLogRow
        data-v2-item-type={item.type}
        icon={<Icon className="size-3.5 text-icon-muted" aria-hidden />}
        label={
          <>
            {item.label} · {display.label}
          </>
        }
      />
    );
  }
  return <PendingSecretRequestForm environmentId={props.environmentId} item={item} />;
}

const NO_SOURCES: ReadonlyArray<SecretRequestSource> = [];

function useSecretRequestSources(environmentId: EnvironmentId) {
  const config = useAtomValue(serverEnvironment.configValueAtom(environmentId));
  return isSecretsFeatureEnabled
    ? secretRequestSources(config, config?.settings.secretProviders ?? {})
    : NO_SOURCES;
}

function PendingSecretRequestForm(props: {
  readonly environmentId: EnvironmentId;
  readonly item: SecretRequestItem;
}) {
  const { item } = props;
  const inputId = useId();
  const errorId = useId();
  const privacyId = useId();
  const answer = useAtomCommand(serverEnvironment.answerSecretRequest, {
    label: "answer secret request",
    // The failure cause holds the request; keep it out of the console.
    reportFailure: false,
    reportDefect: false,
  });
  const configuredSources = useSecretRequestSources(props.environmentId);
  const sources =
    item.fields === undefined && item.destination === undefined ? configuredSources : NO_SOURCES;
  const [sourceId, setSourceId] = useState<SecretProviderId | null>(null);
  const source = sources.find((candidate) => candidate.id === sourceId) ?? null;
  const [reference, setReference] = useState("");
  const fields = secretRequestFields(item);
  const fieldId = (index: number) => `${inputId}-${index}`;
  const [values, setValues] = useState<ReadonlyMap<string, string>>(() => new Map());
  const complete = secretRequestComplete(item, values);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Enter and click can both submit before a render.
  const inFlight = useRef(false);

  const send = async (reply: SecretRequestReply) => {
    const input = secretRequestAnswerInput(item, reply);
    if (input === null || inFlight.current) return;
    inFlight.current = true;
    setSubmitting(true);
    setError(null);
    const result = await answer({ environmentId: props.environmentId, input }).finally(() => {
      inFlight.current = false;
      setSubmitting(false);
    });
    if (result._tag === "Success") {
      setReference("");
      setValues(new Map());
      return;
    }
    if (!isAtomCommandInterrupted(result)) {
      pipe(squashAtomCommandFailure(result), secretRequestFailureMessage, setError);
    }
  };

  const onSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (source === null) {
      void send({ type: "save", values });
    } else if (secretRequestReference(source, reference) === undefined) {
      setError(source.invalidReferenceMessage);
    } else {
      void send({ type: "reference", source, reference });
    }
  };
  const link = secretRequestLink(item);
  const describedBy = error !== null ? `${privacyId} ${errorId}` : privacyId;

  return (
    <form
      data-v2-item-type={item.type}
      className="flex min-w-0 flex-col gap-3 rounded-xl border border-border/60 bg-card p-4"
      onSubmit={onSubmit}
      autoComplete="off"
      // On click, not pointerdown: the mousedown that follows a pointerdown
      // moves focus back off the input. A click that ends a text selection
      // leaves it alone so the reason can still be copied.
      onClick={(event) => {
        if (!(event.target instanceof Element)) return;
        if (event.target.closest("input, button, a")) return;
        if (window.getSelection()?.isCollapsed === false) return;
        document.getElementById(fieldId(0))?.focus();
      }}
    >
      <div className="flex min-w-0 flex-col gap-1">
        {/* With several fields, each input carries its own label. */}
        {item.fields === undefined ? (
          <label htmlFor={fieldId(0)} className="text-sm font-medium text-foreground">
            {item.label}
          </label>
        ) : (
          <p className="text-sm font-medium text-foreground">{item.label}</p>
        )}
        {item.reason.trim() ? (
          <p className="text-sm wrap-anywhere text-muted-foreground">{item.reason}</p>
        ) : null}
      </div>
      {link !== null ? (
        <div className="flex min-w-0 flex-col items-start gap-1.5">
          <Button
            variant="outline"
            size="sm"
            render={<a href={link.href} target="_blank" rel="noopener noreferrer" />}
          >
            Open {link.host}
            <ExternalLinkIcon aria-hidden />
          </Button>
          {/* A link found in the reason is already shown there. */}
          {item.url !== undefined ? (
            <p className="font-mono text-xs wrap-anywhere text-muted-foreground">{item.url}</p>
          ) : null}
        </div>
      ) : null}
      <div className="flex min-w-0 items-end gap-2">
        {sources.length > 0 ? (
          <SecretSourceSelect
            value={source?.id ?? null}
            sources={sources}
            label={item.label}
            disabled={submitting}
            onValueChange={(next) => {
              setSourceId(next);
              setError(null);
            }}
          />
        ) : null}
        <div className="flex min-w-0 flex-1 flex-col gap-2">
          {source === null ? (
            <>
              {fields.map((field, index) => (
                <div key={field.name} className="flex min-w-0 flex-col gap-1">
                  {/* A single field is titled by the card itself. */}
                  {item.fields === undefined ? null : (
                    <label htmlFor={fieldId(index)} className="text-xs text-muted-foreground">
                      {field.label}
                    </label>
                  )}
                  <Input
                    id={fieldId(index)}
                    type="password"
                    autoComplete="off"
                    autoCorrect="off"
                    autoCapitalize="off"
                    spellCheck={false}
                    // Password managers otherwise offer to save or fill this field.
                    data-1p-ignore
                    data-lpignore="true"
                    data-bwignore
                    placeholder={field.placeholder ?? SECRET_REQUEST_DEFAULT_PLACEHOLDER}
                    value={values.get(field.name) ?? ""}
                    disabled={submitting}
                    aria-invalid={error !== null || undefined}
                    aria-describedby={error !== null ? `${privacyId} ${errorId}` : privacyId}
                    onChange={(event) => {
                      const value = event.currentTarget.value;
                      setValues((current) => new Map(current).set(field.name, value));
                    }}
                  />
                </div>
              ))}
            </>
          ) : (
            <Input
              id={fieldId(0)}
              font="mono"
              autoComplete="off"
              autoCorrect="off"
              autoCapitalize="off"
              spellCheck={false}
              placeholder={source.referencePlaceholder}
              value={reference}
              disabled={submitting}
              aria-invalid={error !== null || undefined}
              aria-describedby={describedBy}
              onChange={(event) => {
                setReference(event.currentTarget.value);
                setError(null);
              }}
            />
          )}
        </div>
        <Button
          type="submit"
          disabled={submitting || (source === null ? !complete : reference.trim().length === 0)}
        >
          Save securely
        </Button>
      </div>
      {error !== null ? (
        <p id={errorId} role="alert" className="text-xs text-destructive">
          {error}
        </p>
      ) : null}
      <div className="flex min-w-0 items-center justify-between gap-2">
        <p
          id={privacyId}
          className="flex min-w-0 items-center gap-1.5 text-xs text-muted-foreground"
        >
          <ShieldCheckIcon className="size-3.5 shrink-0" aria-hidden />
          {secretRequestPrivacyNote(item)}
        </p>
        <Button
          type="button"
          size="xs"
          variant="ghost-muted"
          disabled={submitting}
          onClick={() => void send({ type: "decline" })}
        >
          Decline
        </Button>
      </div>
    </form>
  );
}
