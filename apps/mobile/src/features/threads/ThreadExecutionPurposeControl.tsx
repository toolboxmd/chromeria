import { useAtomValue } from "@effect/atom-react";
import type {
  EnvironmentThread,
  EnvironmentThreadShell,
} from "@t3tools/client-runtime/state/shell";
import {
  purposeSelectionOf,
  type OrchestrationV2Run,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import { Atom } from "effect/reactivity";
import { environmentThreadDetails, threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { ExecutionPurposePicker } from "./ExecutionPurposePicker";

const emptyThread = Atom.make<EnvironmentThread | null>(null);
const emptyRuns: ReadonlyArray<OrchestrationV2Run> = [];
const selectRuns = (thread: EnvironmentThread | null) => thread?.projection.runs ?? emptyRuns;

export function ThreadExecutionPurposeControl(props: {
  readonly thread: EnvironmentThreadShell;
  readonly threadRef: ScopedThreadRef | null;
  readonly disabled: boolean;
}) {
  const runs = useAtomValue(
    props.threadRef === null ? emptyThread : environmentThreadDetails.threadAtom(props.threadRef),
    selectRuns,
  );
  const update = useAtomCommand(threadEnvironment.updateMetadata, { reportFailure: true });
  const selection = purposeSelectionOf(props.thread);
  return (
    <ExecutionPurposePicker
      scope={selection.scope}
      runs={runs}
      disabled={props.disabled || props.threadRef === null}
      onChange={(scope) => {
        void update({
          environmentId: props.thread.environmentId,
          input: {
            threadId: props.thread.id,
            purposeSelection: { expectedRevision: selection.revision, scope },
          },
        });
      }}
    />
  );
}
