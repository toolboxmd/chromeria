// @effect-diagnostics globalTimers:off - This main-process WebSocket owns its timeout and event lifecycle.
import {
  COOKIE_MIRROR_MAX_MUTATIONS,
  CookieMirrorReply,
  cookieMirrorKey,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import type { CookieMirror } from "./CookieMirror.ts";

const decodeReply = Schema.decodeUnknownOption(Schema.fromJsonString(CookieMirrorReply));

export class CookieMirrorSocket {
  private readonly socket: WebSocket;
  private ready = false;
  private flushing = false;
  private dirty = true;
  private sequence = 0;
  private drainTask: Promise<void> = Promise.resolve();
  private readonly epochs = new Map<string, number>();
  private readonly sent = new Map<string, string>();
  private readonly profiles = new Map<string, string>();
  private pending:
    | { readonly sequence: number; readonly resolve: () => void; readonly reject: () => void }
    | undefined;
  private timeout: ReturnType<typeof setTimeout> | undefined;

  private readonly mirror: CookieMirror;
  private readonly environmentId: string;

  constructor(url: string, environmentId: string, mirror: CookieMirror) {
    this.mirror = mirror;
    this.environmentId = environmentId;
    this.socket = new WebSocket(url);
    this.armTimeout();
    this.socket.addEventListener("message", (event) => {
      const reply = decodeReply(event.data);
      if (reply._tag === "None") return this.close();
      if (reply.value.type === "ready") {
        if (this.ready || reply.value.environmentId !== environmentId) return this.close();
        this.ready = true;
        clearTimeout(this.timeout);
        this.flush();
      } else if (reply.value.type === "resync") {
        const profileId = reply.value.profileId;
        this.epochs.set(profileId, (this.epochs.get(profileId) ?? 0) + 1);
        for (const key of this.sent.keys())
          if (key.startsWith(`[${JSON.stringify(profileId)},`)) this.sent.delete(key);
        this.profiles.delete(profileId);
        this.flush();
      } else if (reply.value.sequence === this.pending?.sequence) {
        clearTimeout(this.timeout);
        this.pending.resolve();
        this.pending = undefined;
      } else this.close();
    });
    this.socket.addEventListener("close", () => this.closed());
    this.socket.addEventListener("error", () => this.close());
  }

  get connected() {
    return (
      this.socket.readyState === WebSocket.OPEN || this.socket.readyState === WebSocket.CONNECTING
    );
  }

  private armTimeout() {
    clearTimeout(this.timeout);
    this.timeout = setTimeout(() => this.close(), 15_000);
  }

  close() {
    this.closed();
    this.socket.close();
  }

  private closed() {
    this.ready = false;
    clearTimeout(this.timeout);
    this.pending?.reject();
    this.pending = undefined;
  }

  flush(): Promise<void> {
    this.dirty = true;
    if (!this.ready || this.flushing) return this.drainTask;
    this.flushing = true;
    this.drainTask = this.drain()
      .catch(() => this.close())
      .finally(() => {
        this.flushing = false;
        if (this.dirty && this.ready) void this.flush();
      });
    return this.drainTask;
  }

  private async drain() {
    while (this.dirty && this.ready) {
      this.dirty = false;
      for (const { profile, mutations } of this.mirror.snapshot(this.environmentId)) {
        const epoch = this.epochs.get(profile.id) ?? 0;
        const changed = mutations.filter((mutation) => {
          const key = JSON.stringify([
            profile.id,
            cookieMirrorKey(mutation.type === "set" ? mutation.cookie : mutation.key),
          ]);
          return this.sent.get(key) !== JSON.stringify(mutation);
        });
        if (changed.length === 0 && this.profiles.get(profile.id) === profile.name) continue;
        for (
          let offset = 0;
          offset < Math.max(changed.length, 1);
          offset += COOKIE_MIRROR_MAX_MUTATIONS
        ) {
          if (!this.ready) throw new Error("Browser mirror disconnected.");
          const batch = changed.slice(offset, offset + COOKIE_MIRROR_MAX_MUTATIONS);
          await this.mirror.prepareDelivery(this.environmentId, profile.id, batch);
          const sequence = this.sequence++;
          const acknowledged = new Promise<void>((resolve, reject) => {
            this.pending = {
              sequence,
              resolve,
              reject: () => reject(new Error("Browser mirror disconnected.")),
            };
          });
          this.armTimeout();
          this.socket.send(
            JSON.stringify({
              environmentId: this.environmentId,
              sequence,
              profile,
              mutations: batch,
            }),
          );
          await acknowledged;
          await this.mirror.acknowledge(this.environmentId, profile.id, batch);
          if ((this.epochs.get(profile.id) ?? 0) !== epoch) break;
          for (const mutation of batch) {
            const key = JSON.stringify([
              profile.id,
              cookieMirrorKey(mutation.type === "set" ? mutation.cookie : mutation.key),
            ]);
            this.sent.set(key, JSON.stringify(mutation));
          }
        }
        if ((this.epochs.get(profile.id) ?? 0) === epoch)
          this.profiles.set(profile.id, profile.name);
      }
    }
  }
}
