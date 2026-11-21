import {
  BROWSER_PROFILE_MAX_COUNT,
  cookieMirrorKey,
  resolveBrowserProfiles,
  type BrowserProfile,
  type CookieMirrorKey,
  type CookieMirrorMutation,
  type CookieMirrorProfile,
  type MirrorCookie,
} from "@t3tools/contracts";
import {
  applyMirrorMutations,
  diffMirrorCookies,
  type CookieMirrorStore,
} from "@t3tools/shared/browserCookieMirror";

import {
  emptyCookieMirrorMetadata,
  type CookieMirrorMetadata,
  type ManagedCookieKey,
  type PartitionMetadata,
} from "./CookieMirrorMetadata.ts";

export interface ChromeCookieSnapshot {
  readonly cookies: ReadonlyArray<MirrorCookie>;
  readonly identities: ReadonlyArray<ManagedCookieKey>;
  readonly complete: boolean;
  readonly processIdentity?: string;
  readonly unresolvedPartitions?: ReadonlyArray<
    Pick<CookieMirrorKey, "name" | "domain" | "path"> & { readonly topLevelSite: string }
  >;
}
interface ChromeState {
  processIdentity: string | undefined;
  readonly profileId: string;
  readonly managed: Map<string, ManagedCookieKey>;
  readonly removed: Map<string, CookieMirrorKey>;
  readonly cookies: Map<string, MirrorCookie>;
}
interface PartitionState {
  readonly id: string;
  readonly environmentId: string;
  profile: CookieMirrorProfile;
  readonly managed: Map<string, ManagedCookieKey>;
  readonly chromeManaged: Map<string, CookieMirrorKey>;
  readonly remoteManaged: Map<string, CookieMirrorKey>;
  readonly removed: Map<string, CookieMirrorKey>;
  readonly cookies: Map<string, MirrorCookie>;
  readonly pending: Map<string, CookieMirrorMutation>;
  store: CookieMirrorStore | undefined;
  changed: boolean;
}
const keyOfMutation = (mutation: CookieMirrorMutation) =>
  cookieMirrorKey(mutation.type === "set" ? mutation.cookie : mutation.key);
const identity = (cookie: MirrorCookie): ManagedCookieKey => ({
  key: {
    name: cookie.name,
    domain: cookie.domain,
    path: cookie.path,
    ...(cookie.partitionKey ? { partitionKey: cookie.partitionKey } : {}),
  },
  persistent: cookie.expires !== undefined,
});
const keys = (items: ReadonlyArray<CookieMirrorKey>) =>
  new Map(items.map((key) => [cookieMirrorKey(key), key]));
const managedKeys = (items: ReadonlyArray<ManagedCookieKey>) =>
  new Map(items.map((item) => [cookieMirrorKey(item.key), item]));

/** Values live only in the browser and these process-local observations. */
export class CookieMirror {
  private readonly partitions = new Map<string, PartitionState>();
  private readonly chrome = new Map<string, ChromeState>();
  private mappings: CookieMirrorMetadata["mappings"];
  private readonly changed: (environmentId: string) => void;
  private readonly persist: (metadata: CookieMirrorMetadata) => Promise<void>;
  private tail: Promise<unknown> = Promise.resolve();
  constructor(
    options: {
      readonly metadata?: CookieMirrorMetadata;
      readonly changed?: (environmentId: string) => void;
      readonly persist?: (metadata: CookieMirrorMetadata) => Promise<void>;
    } = {},
  ) {
    const metadata = options.metadata ?? emptyCookieMirrorMetadata();
    this.changed = options.changed ?? (() => {});
    this.persist = options.persist ?? (async () => {});
    this.mappings = metadata.mappings;
    for (const source of metadata.chrome)
      this.chrome.set(source.source, {
        processIdentity: undefined,
        profileId: source.profileId,
        managed: managedKeys(source.managed),
        removed: keys(source.removed),
        cookies: new Map(),
      });
    for (const partition of metadata.partitions)
      this.partitions.set(partition.id, this.restorePartition(partition));
  }
  private restorePartition(partition: PartitionMetadata): PartitionState {
    return {
      ...partition,
      managed: managedKeys(partition.managed),
      chromeManaged: keys(partition.chromeManaged),
      remoteManaged: keys(partition.remoteManaged),
      removed: keys(partition.removed),
      cookies: new Map(),
      pending: new Map(),
      store: undefined,
      changed: false,
    };
  }
  private serial<A>(work: () => Promise<A>): Promise<A> {
    const result = this.tail.then(work);
    this.tail = result.catch(() => undefined);
    return result;
  }
  metadata(): CookieMirrorMetadata {
    return {
      version: 1,
      mappings: this.mappings,
      chrome: [...this.chrome].map(([source, state]) => ({
        source,
        profileId: state.profileId,
        managed: [...state.managed.values()],
        removed: [...state.removed.values()],
      })),
      partitions: [...this.partitions.values()].map((state) => ({
        id: state.id,
        environmentId: state.environmentId,
        profile: state.profile,
        managed: [...state.managed.values()],
        chromeManaged: [...state.chromeManaged.values()],
        remoteManaged: [...state.remoteManaged.values()],
        removed: [...state.removed.values()],
      })),
    };
  }
  private async save() {
    await this.persist(this.metadata());
    for (const partition of this.partitions.values())
      if (partition.changed) {
        partition.changed = false;
        this.changed(partition.environmentId);
      }
  }
  mapChromeProfile(input: {
    readonly root: string;
    readonly directory: string;
    readonly name: string;
    readonly proposedId: string;
    readonly profiles: ReadonlyArray<BrowserProfile>;
  }) {
    return this.serial(async () => {
      const existing = this.mappings.find(
        (mapping) => mapping.root === input.root && mapping.directory === input.directory,
      );
      const id =
        input.directory === "Default" ? "default" : (existing?.profileId ?? input.proposedId);
      const profiles = resolveBrowserProfiles(input.profiles);
      if (
        !profiles.some((profile) => profile.id === id) &&
        profiles.length >= BROWSER_PROFILE_MAX_COUNT
      )
        throw new Error("Browser mirror profile limit reached.");
      const name = input.name.trim() || input.directory;
      let label = name.slice(0, 48);
      let disambiguator = 1;
      while (profiles.some((profile) => profile.id !== id && profile.name === label)) {
        const suffix =
          ` (${input.directory}${disambiguator === 1 ? "" : ` ${disambiguator}`})`.slice(-46);
        label = `${name.slice(0, 48 - suffix.length)}${suffix}`;
        disambiguator += 1;
      }
      const retained = profiles.find((profile) => profile.id === id);
      const profile: CookieMirrorProfile = {
        id,
        name: retained?.name ?? label,
        kind: "persistent",
      };
      const next = profiles.map((old) => (old.id === id ? profile : old));
      if (!profiles.some((old) => old.id === id)) next.push(profile);
      if (!existing)
        this.mappings = [
          ...this.mappings,
          { root: input.root, directory: input.directory, profileId: id },
        ];
      await this.save();
      return { profile, profiles: next };
    });
  }
  private record(partition: PartitionState, mutations: ReadonlyArray<CookieMirrorMutation>) {
    applyMirrorMutations(partition.cookies, mutations);
    for (const mutation of mutations) {
      const key = keyOfMutation(mutation);
      if (mutation.type === "set") {
        partition.managed.set(key, identity(mutation.cookie));
        partition.removed.delete(key);
      } else {
        partition.managed.delete(key);
        partition.removed.set(key, mutation.key);
      }
    }
    if (mutations.length > 0) partition.changed = true;
  }
  private async applyPending(partition: PartitionState) {
    if (!partition.store || partition.pending.size === 0) return;
    const mutations = [...partition.pending.values()];
    // Persist write intent: a browser can apply a write before its reply is lost.
    for (const mutation of mutations)
      if (mutation.type === "set")
        partition.chromeManaged.set(keyOfMutation(mutation), identity(mutation.cookie).key);
    await this.save();
    await partition.store.apply(mutations);
    for (const mutation of mutations)
      if (mutation.type === "remove") partition.chromeManaged.delete(keyOfMutation(mutation));
    this.record(partition, mutations);
    await this.save();
    partition.pending.clear();
  }
  addPartition(
    id: string,
    environmentId: string,
    profile: CookieMirrorProfile,
    store: CookieMirrorStore,
  ) {
    return this.serial(async () => {
      if (profile.id === "incognito") return;
      let partition = this.partitions.get(id);
      if (partition?.store === store) {
        if (partition.profile.name !== profile.name) partition.changed = true;
        partition.profile = profile;
        await this.save();
        return;
      }
      const cookies = await store.read();
      partition ??= this.restorePartition({
        id,
        environmentId,
        profile,
        managed: [],
        chromeManaged: [],
        remoteManaged: [],
        removed: [],
      });
      partition.store = store;
      partition.profile = profile;
      const current = new Set(cookies.map(cookieMirrorKey));
      // Startup cannot distinguish a session ending from a logout while we were off.
      for (const [key, old] of partition.managed)
        if (!current.has(key)) {
          if (old.persistent) this.record(partition, [{ type: "remove", key: old.key }]);
          else partition.managed.delete(key);
        }
      this.partitions.set(id, partition);
      this.record(
        partition,
        cookies.map((cookie) => ({ type: "set", cookie })),
      );
      await this.save();
      for (const source of this.chrome.values())
        if (source.profileId === profile.id) {
          for (const cookie of source.cookies.values())
            partition.pending.set(cookieMirrorKey(cookie), { type: "set", cookie });
          for (const key of source.removed.values())
            if (partition.chromeManaged.has(cookieMirrorKey(key)))
              partition.pending.set(cookieMirrorKey(key), { type: "remove", key });
        }
      await this.applyPending(partition);
    });
  }
  observeDesktop(id: string) {
    return this.serial(async () => {
      const partition = this.partitions.get(id);
      if (!partition?.store) return;
      await this.applyPending(partition);
      const cookies = await partition.store.read();
      this.record(partition, diffMirrorCookies(partition.cookies, cookies));
      await this.save();
    });
  }
  observeChrome(sourceId: string, profile: CookieMirrorProfile, snapshot: ChromeCookieSnapshot) {
    return this.serial(async () => {
      if (profile.id === "incognito") return [];
      const source = this.chrome.get(sourceId) ?? {
        processIdentity: undefined,
        profileId: profile.id,
        managed: new Map<string, ManagedCookieKey>(),
        removed: new Map<string, CookieMirrorKey>(),
        cookies: new Map<string, MirrorCookie>(),
      };
      this.chrome.set(sourceId, source);
      const mutations: CookieMirrorMutation[] = diffMirrorCookies(
        source.cookies,
        snapshot.cookies,
      ).filter((mutation) => mutation.type === "set");
      const present = new Set(snapshot.identities.map((item) => cookieMirrorKey(item.key)));
      for (const [key, previous] of source.managed) {
        const unresolved =
          previous.key.partitionKey &&
          snapshot.unresolvedPartitions?.some(
            (unknown) =>
              unknown.name === previous.key.name &&
              unknown.domain === previous.key.domain &&
              unknown.path === previous.key.path &&
              unknown.topLevelSite === previous.key.partitionKey?.topLevelSite,
          );
        if (!present.has(key) && !unresolved) {
          // A lock alone does not distinguish startup cleanup from signing out.
          if (
            previous.persistent ||
            (snapshot.processIdentity !== undefined &&
              source.processIdentity === snapshot.processIdentity)
          ) {
            source.removed.set(key, previous.key);
            mutations.push({ type: "remove", key: previous.key });
          }
          source.managed.delete(key);
          source.cookies.delete(key);
        }
      }
      source.processIdentity = snapshot.processIdentity;
      for (const cookie of snapshot.cookies) {
        const key = cookieMirrorKey(cookie);
        source.managed.set(key, identity(cookie));
        source.removed.delete(key);
        source.cookies.set(key, cookie);
      }
      for (const partition of this.partitions.values()) {
        if (partition.profile.id !== profile.id) continue;
        for (const mutation of mutations)
          if (mutation.type === "set" || partition.chromeManaged.has(keyOfMutation(mutation)))
            partition.pending.set(keyOfMutation(mutation), mutation);
        for (const key of source.removed.values())
          if (partition.chromeManaged.has(cookieMirrorKey(key)))
            partition.pending.set(cookieMirrorKey(key), { type: "remove", key });
      }
      await this.save();
      const failures: string[] = [];
      for (const partition of this.partitions.values())
        if (partition.profile.id === profile.id) {
          try {
            await this.applyPending(partition);
          } catch {
            failures.push(partition.id);
          }
        }
      return failures;
    });
  }
  prepareDelivery(
    environmentId: string,
    profileId: string,
    mutations: ReadonlyArray<CookieMirrorMutation>,
  ) {
    return this.serial(async () => {
      const partition = [...this.partitions.values()].find(
        (state) => state.environmentId === environmentId && state.profile.id === profileId,
      );
      if (!partition) return;
      for (const mutation of mutations)
        if (mutation.type === "set")
          partition.remoteManaged.set(keyOfMutation(mutation), identity(mutation.cookie).key);
      await this.save();
    });
  }
  acknowledge(
    environmentId: string,
    profileId: string,
    mutations: ReadonlyArray<CookieMirrorMutation>,
  ) {
    return this.serial(async () => {
      const partition = [...this.partitions.values()].find(
        (state) => state.environmentId === environmentId && state.profile.id === profileId,
      );
      if (!partition) return;
      for (const mutation of mutations)
        if (mutation.type === "remove") {
          const key = keyOfMutation(mutation);
          partition.remoteManaged.delete(key);
          if (!partition.cookies.has(key)) partition.removed.delete(key);
        }
      await this.save();
    });
  }
  snapshot(environmentId: string) {
    return [...this.partitions.values()]
      .filter((state) => state.store && state.environmentId === environmentId)
      .map((state) => ({
        profile: state.profile,
        mutations: [
          ...[...state.cookies.values()].map((cookie): CookieMirrorMutation => ({
            type: "set",
            cookie,
          })),
          ...[...state.removed]
            .filter(([key]) => state.remoteManaged.has(key))
            .map(([, key]): CookieMirrorMutation => ({ type: "remove", key })),
        ],
      }));
  }
}
