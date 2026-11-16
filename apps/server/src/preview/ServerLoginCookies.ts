import {
  INCOGNITO_BROWSER_PROFILE_ID,
  OrchestratorMcpFailure,
  type BrowserProfile,
  type MirrorCookie,
  type PreviewLoginExportResult,
  type PreviewLoginImportResult,
} from "@t3tools/contracts";
import { planLoginImport, selectLoginCookies } from "@t3tools/shared/browserLoginImport";

import { registrableDomain } from "./CredentialFill.ts";
import type { ServerCookieMirror } from "./ServerCookieMirror.ts";

export type LoginRefusal =
  | "unknown_profile"
  | "incognito_profile"
  | "login_in_progress"
  | "device_bound"
  | "too_many_cookies";

/** Refusals name the reason first and carry nothing read from a profile. */
export const loginRefusal = (reason: LoginRefusal, profileId?: string) =>
  new OrchestratorMcpFailure({
    code: "invalid_request",
    message: {
      unknown_profile: `unknown_profile: No browser profile has id "${profileId}". Use an id preview_status lists.`,
      incognito_profile: "incognito_profile: An incognito profile keeps no logins to copy.",
      login_in_progress: `login_in_progress: Profile "${profileId}" is in a login. End it with preview_end_login first.`,
      device_bound:
        "device_bound: Google sign-in is bound to each machine. Sign in here with preview_fill_credential.",
      too_many_cookies:
        "too_many_cookies: The listed hosts hold more than 256 cookies or 64 KiB. List fewer hosts.",
    }[reason],
  });

export interface LoginProfiles {
  /** The known profile with exactly this id; names are not accepted here. */
  readonly find: (profileId: string) => BrowserProfile | undefined;
  readonly inLogin: (profileId: string) => boolean;
}

/** Runs before any browser context opens, so a refused id creates nothing. */
const checkLoginProfile = (profiles: LoginProfiles, profileId: string) => {
  const profile = profiles.find(profileId);
  if (profile === undefined) return loginRefusal("unknown_profile", profileId);
  if (profile.kind === "incognito" || profileId === INCOGNITO_BROWSER_PROFILE_ID)
    return loginRefusal("incognito_profile");
  if (profiles.inLogin(profileId)) return loginRefusal("login_in_progress", profileId);
  return undefined;
};

/** Copies one bounded snapshot of a profile's cookies out of, or into, this server's browser. */
export class ServerLoginCookies {
  private readonly mirror: ServerCookieMirror;
  private readonly profiles: LoginProfiles;
  constructor(mirror: ServerCookieMirror, profiles: LoginProfiles) {
    this.mirror = mirror;
    this.profiles = profiles;
  }

  check(profileId: string) {
    const refused = checkLoginProfile(this.profiles, profileId);
    if (refused !== undefined) throw refused;
  }

  async export(input: {
    readonly profileId: string;
    readonly hosts: ReadonlyArray<string>;
  }): Promise<PreviewLoginExportResult> {
    this.check(input.profileId);
    const cookies = await this.mirror.readProfile(input.profileId);
    const selection = selectLoginCookies({ cookies, hosts: input.hosts, registrableDomain });
    if (selection._tag === "refused") throw loginRefusal(selection.reason);
    return { cookies: selection.cookies, skipped: selection.skipped };
  }

  async import(input: {
    readonly profileId: string;
    readonly hosts: ReadonlyArray<string>;
    readonly cookies: ReadonlyArray<MirrorCookie>;
    readonly skipped: PreviewLoginExportResult["skipped"];
  }): Promise<PreviewLoginImportResult> {
    this.check(input.profileId);
    return this.mirror.writeProfile(input.profileId, (current) => {
      // A login may have started while the context opened.
      this.check(input.profileId);
      const plan = planLoginImport({
        current,
        snapshot: input.cookies,
        hosts: input.hosts,
        registrableDomain,
      });
      if (plan._tag === "refused") throw loginRefusal(plan.reason);
      return {
        mutations: plan.mutations,
        result: {
          profileId: input.profileId,
          imported: plan.imported,
          skipped: { ...input.skipped, sharedDomain: plan.sharedDomain },
        },
      };
    });
  }
}
