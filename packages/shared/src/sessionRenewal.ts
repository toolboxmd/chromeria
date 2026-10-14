import { sha256 } from "@noble/hashes/sha2";

/** SHA-256 commitment, usable on direct HTTP browser origins without SubtleCrypto. */
export function renewalCommitment(secret: string): string {
  return Array.from(sha256(new TextEncoder().encode(secret)), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
}
