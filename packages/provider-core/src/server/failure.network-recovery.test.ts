import { assert, it } from "@effect/vitest";
import { makeProviderFailure } from "./failure.ts";

it.each([
  "ENOTFOUND",
  "EAI_AGAIN",
  "ECONNREFUSED",
  "ECONNRESET",
  "ENETUNREACH",
  "EHOSTUNREACH",
  "ETIMEDOUT",
])("classifies nested network cause %s without losing sanitization", (code) => {
  const failure = makeProviderFailure({
    class: "provider_error",
    cause: { cause: { code } },
    message: "Provider request failed https://api.test?token=secret",
  });
  assert.equal(failure.class, "transport_error");
  assert.equal(failure.retryable, true);
  assert.notInclude(failure.message, "secret");
});

it.each([
  "httpConnectionFailed",
  "responseStreamConnectionFailed",
  "responseStreamDisconnected",
  "responseStreamTooManyFailedAttempts",
])("classifies Codex network code %s even after native retries stop", (code) => {
  const failure = makeProviderFailure({
    class: "provider_error",
    code,
    message: "Provider stopped retrying",
  });
  assert.equal(failure.class, "transport_error");
  assert.equal(failure.retryable, true);
});

it.each([
  "API Error: Can't reach the API server",
  "fetch failed",
  "Error: ConnectError: [unavailable] socket closed",
])("classifies explicit provider network diagnostic %s", (message) => {
  const failure = makeProviderFailure({ class: "provider_error", cause: message });
  assert.equal(failure.class, "transport_error");
  assert.equal(failure.retryable, true);
});

it.each([
  "Invalid API key",
  "Internal error",
  "WritableIterable is closed",
  "HTTP 403 Forbidden",
  "Context window exceeded",
])("does not guess network failure from %s", (message) => {
  const failure = makeProviderFailure({ class: "provider_error", message });
  assert.equal(failure.class, "provider_error");
  assert.equal(failure.retryable, null);
});
