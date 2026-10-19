// @effect-diagnostics nodeBuiltinImport:off - Promise adapter for the value-free server profile catalog.
import { CookieMirrorProfile } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

const Profiles = Schema.Struct({
  version: Schema.Literal(1),
  profiles: Schema.Array(CookieMirrorProfile),
});
const decode = Schema.decodeUnknownSync(Schema.fromJsonString(Profiles));
const encode = Schema.encodeSync(Schema.fromJsonString(Profiles));

export class ServerCookieMirrorProfiles {
  private readonly path: string;
  private readonly profiles = new Map<string, typeof CookieMirrorProfile.Type>();
  constructor(path: string) {
    this.path = path;
  }
  async read() {
    try {
      const document = decode(await NodeFSP.readFile(this.path, "utf8"));
      for (const profile of document.profiles) this.profiles.set(profile.id, profile);
    } catch (error) {
      if (error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT")
        return [];
      // oxlint-disable-next-line preserve-caught-error -- Schema errors include the raw document.
      throw new Error("Could not read mirrored browser profile catalog.");
    }
    return [...this.profiles.values()];
  }
  async put(profile: typeof CookieMirrorProfile.Type) {
    if (this.profiles.get(profile.id)?.name === profile.name) return;
    const next = new Map(this.profiles).set(profile.id, profile);
    const temporary = `${this.path}.tmp`;
    try {
      await NodeFSP.mkdir(NodePath.dirname(this.path), { recursive: true });
      await NodeFSP.writeFile(temporary, encode({ version: 1, profiles: [...next.values()] }), {
        mode: 0o600,
      });
      await NodeFSP.rename(temporary, this.path);
      this.profiles.set(profile.id, profile);
    } catch {
      throw new Error("Could not write mirrored browser profile catalog.");
    } finally {
      await NodeFSP.rm(temporary, { force: true }).catch(() => undefined);
    }
  }
}
