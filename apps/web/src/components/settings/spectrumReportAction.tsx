import { useAtomValue } from "@effect/atom-react";
import {
  abandonableReportRun,
  abandonReportFailureMessage,
  abandonReportNotice,
} from "@t3tools/client-runtime/spectrum-report";
import {
  createEnvironmentRpcCommand,
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import {
  type EnvironmentId,
  type ScheduledTask,
  SPECTRUM_WS_METHODS,
  type SpectrumReportAbandonInput,
} from "@t3tools/contracts";
import { useState } from "react";

import { requestConfirmDialog } from "../../confirmDialog";
import { connectionAtomRuntime } from "../../connection/runtime";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";

const abandonReport = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "spectrum:abandon-report",
  tag: SPECTRUM_WS_METHODS.abandonReport,
});

const CONFIRM =
  "Abandon this Spectrum report?\nSpectrum stops delivering it and the run goes on to its check.";

/**
 * Fork (toolboxmd/chromeria#176): **Abandon report** beside a scheduled run
 * that a Spectrum report holds at needs-you. Nothing shows for any other run.
 */
export function AbandonReportAction({
  environmentId,
  task,
}: {
  readonly environmentId: EnvironmentId;
  readonly task: ScheduledTask;
}) {
  const request = abandonableReportRun(task);
  return request === null ? null : (
    // Another held run starts over instead of showing this one's outcome.
    <AbandonReportButton
      key={request.schedulerRunId}
      environmentId={environmentId}
      request={request}
    />
  );
}

type State =
  | { readonly _tag: "idle" }
  | { readonly _tag: "pending" }
  | { readonly _tag: "answered"; readonly notice: string }
  | { readonly _tag: "failed"; readonly message: string };

function AbandonReportButton({
  environmentId,
  request,
}: {
  readonly environmentId: EnvironmentId;
  readonly request: SpectrumReportAbandonInput;
}) {
  const abandon = useAtomCommand(abandonReport, { reportFailure: false });
  const canAbandon = useAtomValue(abandonReport.permissionAtom(environmentId));
  const [state, setState] = useState<State>({ _tag: "idle" });

  const submit = async () => {
    setState({ _tag: "pending" });
    const result = await abandon({ environmentId, input: request });
    if (result._tag === "Success") {
      setState({ _tag: "answered", notice: abandonReportNotice(result.value) });
    } else if (isAtomCommandInterrupted(result)) {
      setState({ _tag: "idle" });
    } else {
      const message = abandonReportFailureMessage(squashAtomCommandFailure(result));
      setState({ _tag: "failed", message });
    }
  };
  const confirmThenSubmit = async () => {
    const confirmed =
      (await requestConfirmDialog(CONFIRM, { variant: "destructive" })) ??
      // No themed dialog host is mounted; ask natively rather than abandon unasked.
      window.confirm(CONFIRM);
    if (confirmed) await submit();
  };

  if (state._tag === "answered") {
    return <span className="text-muted-foreground">{state.notice}</span>;
  }
  if (state._tag === "failed") {
    return (
      <span className="text-destructive">
        {state.message}{" "}
        <Button variant="link" size="compact" disabled={!canAbandon} onClick={() => void submit()}>
          Retry
        </Button>
      </span>
    );
  }
  return (
    <Button
      variant="destructive-outline"
      size="compact"
      disabled={!canAbandon || state._tag === "pending"}
      onClick={() => void confirmThenSubmit()}
    >
      {state._tag === "pending" ? "Abandoning…" : "Abandon report"}
    </Button>
  );
}
