/**
 * People (toolboxmd/chromeria#121, #170): which person's threads the sidebar shows.
 * Ownership is a view, not access control: every view can still open every
 * thread by link.
 */
import { DEFAULT_PERSON, PEOPLE, threadOwner } from "@t3tools/contracts";

export const PERSON_VIEWS = [...PEOPLE, "Shared", "All"] as const;
export type PersonView = (typeof PERSON_VIEWS)[number];

export interface ThreadOwnership {
  readonly owner?: string | null | undefined;
  readonly coOwners?: ReadonlyArray<string> | undefined;
}

/** Threads with no recorded owner predate people and belong to the default person. */
export function threadInPersonView(thread: ThreadOwnership, view: PersonView): boolean {
  switch (view) {
    case "All":
      return true;
    case "Shared":
      return (thread.coOwners ?? []).length > 0;
    default:
      return threadOwner(thread) === view || (thread.coOwners ?? []).includes(view);
  }
}

/**
 * An unsent draft has no server owner yet. The server stamps the device's
 * person when it is sent, so the draft already counts as theirs.
 */
export function draftInPersonView(devicePerson: string, view: PersonView): boolean {
  return threadInPersonView({ owner: devicePerson, coOwners: [] }, view);
}

/**
 * This device's person: the label the primary environment gives this
 * session, else the first label any other environment gives it, else the
 * default person.
 */
export function resolveDevicePerson(
  sessions: ReadonlyArray<{ readonly primary: boolean; readonly person: string | undefined }>,
): string {
  return (
    sessions.find((session) => session.primary && session.person !== undefined)?.person ??
    sessions.find((session) => session.person !== undefined)?.person ??
    DEFAULT_PERSON
  );
}

export function personViewFor(person: string): PersonView {
  return PERSON_VIEWS.find((view) => view === person) ?? DEFAULT_PERSON;
}

export type ThreadSharingAction =
  | { readonly type: "thread.share"; readonly coOwner: string }
  | { readonly type: "thread.unshare" }
  | { readonly type: "thread.leave" };

/**
 * What `person` can do with a thread's sharing: its owner shares it with the
 * other person or unshares it, a co-owner leaves it, anyone else does nothing.
 */
export function threadSharingAction(
  thread: ThreadOwnership,
  person: string,
): ThreadSharingAction | null {
  const owner = threadOwner(thread);
  const coOwners = thread.coOwners ?? [];
  if (person === owner) {
    if (coOwners.length > 0) return { type: "thread.unshare" };
    const coOwner = PEOPLE.find((candidate) => candidate !== owner);
    return coOwner === undefined ? null : { type: "thread.share", coOwner };
  }
  return coOwners.includes(person) ? { type: "thread.leave" } : null;
}
