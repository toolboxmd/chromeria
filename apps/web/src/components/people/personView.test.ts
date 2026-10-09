import { describe, expect, it } from "vite-plus/test";

import {
  draftInPersonView,
  resolveDevicePerson,
  threadInPersonView,
  threadSharingAction,
  type PersonView,
} from "./personView";

const lukes = { owner: "Luke", coOwners: [] };
const paulis = { owner: "Pauli", coOwners: [] };
const legacy = { coOwners: [] };
const nullOwner = { owner: null, coOwners: [] };
const sharedByLuke = { owner: "Luke", coOwners: ["Pauli"] };
// A v2 thread created before people, or imported before its backfill, has neither field.
const unset = {};

function visible(view: PersonView) {
  return Object.entries({ lukes, paulis, legacy, nullOwner, sharedByLuke, unset })
    .filter(([, thread]) => threadInPersonView(thread, view))
    .map(([name]) => name);
}

describe("threadInPersonView", () => {
  it("shows a person their own threads, threads without an owner as Luke's, and threads shared with them", () => {
    expect(visible("Luke")).toEqual(["lukes", "legacy", "nullOwner", "sharedByLuke", "unset"]);
    expect(visible("Pauli")).toEqual(["paulis", "sharedByLuke"]);
  });

  it("shows only threads with co-owners under Shared", () => {
    expect(visible("Shared")).toEqual(["sharedByLuke"]);
  });

  it("shows every thread under All", () => {
    expect(visible("All")).toEqual([
      "lukes",
      "paulis",
      "legacy",
      "nullOwner",
      "sharedByLuke",
      "unset",
    ]);
  });
});

describe("draftInPersonView", () => {
  it("counts an unsent draft as the device's person", () => {
    expect(draftInPersonView("Pauli", "Pauli")).toBe(true);
    expect(draftInPersonView("Pauli", "Luke")).toBe(false);
    expect(draftInPersonView("Pauli", "Shared")).toBe(false);
    expect(draftInPersonView("Pauli", "All")).toBe(true);
    expect(draftInPersonView("Luke", "Luke")).toBe(true);
  });
});

describe("resolveDevicePerson", () => {
  it("prefers the primary environment's label, then any other label, then Luke", () => {
    expect(
      resolveDevicePerson([
        { primary: false, person: "Luke" },
        { primary: true, person: "Pauli" },
      ]),
    ).toBe("Pauli");
    expect(
      resolveDevicePerson([
        { primary: true, person: undefined },
        { primary: false, person: "Pauli" },
      ]),
    ).toBe("Pauli");
    expect(resolveDevicePerson([{ primary: true, person: undefined }])).toBe("Luke");
    expect(resolveDevicePerson([])).toBe("Luke");
  });
});

describe("threadSharingAction", () => {
  it("lets the owner share with the other person, then unshare", () => {
    expect(threadSharingAction(lukes, "Luke")).toEqual({ type: "thread.share", coOwner: "Pauli" });
    expect(threadSharingAction(paulis, "Pauli")).toEqual({
      type: "thread.share",
      coOwner: "Luke",
    });
    expect(threadSharingAction(legacy, "Luke")).toEqual({ type: "thread.share", coOwner: "Pauli" });
    expect(threadSharingAction(sharedByLuke, "Luke")).toEqual({ type: "thread.unshare" });
  });

  it("lets a co-owner leave and gives anyone else no action", () => {
    expect(threadSharingAction(sharedByLuke, "Pauli")).toEqual({ type: "thread.leave" });
    expect(threadSharingAction(lukes, "Pauli")).toBeNull();
    expect(threadSharingAction(legacy, "Pauli")).toBeNull();
  });
});
