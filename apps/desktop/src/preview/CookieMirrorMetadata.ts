// @effect-diagnostics nodeBuiltinImport:off - Promise adapter for the mirror's atomic, value-free metadata file.
import { CookieMirrorKey, CookieMirrorProfile } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

export const ManagedCookieKey = Schema.Struct({ key: CookieMirrorKey, persistent: Schema.Boolean });
export type ManagedCookieKey = typeof ManagedCookieKey.Type;
const ChromeSourceMetadata = Schema.Struct({
  source: Schema.String,
  profileId: Schema.String,
  managed: Schema.Array(ManagedCookieKey),
  removed: Schema.Array(CookieMirrorKey),
});
const PartitionMetadata = Schema.Struct({
  id: Schema.String,
  environmentId: Schema.String,
  profile: CookieMirrorProfile,
  managed: Schema.Array(ManagedCookieKey),
  chromeManaged: Schema.Array(CookieMirrorKey),
  remoteManaged: Schema.Array(CookieMirrorKey),
  removed: Schema.Array(CookieMirrorKey),
});
export type PartitionMetadata = typeof PartitionMetadata.Type;
export const CookieMirrorMetadata = Schema.Struct({
  version: Schema.Literal(1),
  mappings: Schema.Array(
    Schema.Struct({ root: Schema.String, directory: Schema.String, profileId: Schema.String }),
  ),
  chrome: Schema.Array(ChromeSourceMetadata),
  partitions: Schema.Array(PartitionMetadata),
});
export type CookieMirrorMetadata = typeof CookieMirrorMetadata.Type;
const decode = Schema.decodeUnknownSync(Schema.fromJsonString(CookieMirrorMetadata));
const encode = Schema.encodeSync(Schema.fromJsonString(CookieMirrorMetadata));
export const emptyCookieMirrorMetadata = (): CookieMirrorMetadata => ({
  version: 1,
  mappings: [],
  chrome: [],
  partitions: [],
});

export class CookieMirrorMetadataFile {
  private readonly path: string;
  private previous: string | undefined;
  constructor(path: string) {
    this.path = path;
  }
  async read(): Promise<CookieMirrorMetadata> {
    try {
      const metadata = decode(await NodeFSP.readFile(this.path, "utf8"));
      this.previous = encode(metadata);
      return metadata;
    } catch (error) {
      if (error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT")
        return emptyCookieMirrorMetadata();
      // oxlint-disable-next-line preserve-caught-error -- Schema errors include the raw document.
      throw new Error("Could not read browser mirror metadata.");
    }
  }
  async write(metadata: CookieMirrorMetadata) {
    const encoded = encode(metadata);
    if (encoded === this.previous) return;
    const temporary = `${this.path}.tmp`;
    try {
      await NodeFSP.mkdir(NodePath.dirname(this.path), { recursive: true });
      await NodeFSP.writeFile(temporary, encoded, { mode: 0o600 });
      await NodeFSP.rename(temporary, this.path);
      this.previous = encoded;
    } catch {
      throw new Error("Could not write browser mirror metadata.");
    } finally {
      await NodeFSP.rm(temporary, { force: true }).catch(() => undefined);
    }
  }
}
