import * as Layer from "effect/Layer";
import * as Admission from "./providerAdmissionGate.ts";
import * as Starts from "./forkProviderStartAdmission.ts";
import * as Maintenance from "./providerMaintenanceRunner.ts";

// Server composition and its integration tests use this exact layer object.
// Effect memoizes Admission.layer, so starts and maintenance share one gate.
export const layer = Layer.mergeAll(Starts.layer, Maintenance.layer).pipe(
  Layer.provideMerge(Admission.layer),
);
