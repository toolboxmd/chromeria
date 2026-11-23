/**
 * The words his status line shows for a step: its title up to the first path, command or code,
 * so the line reads like a messenger's ("Read", "Searching the web"), never a file path.
 * Null when nothing plain is left; the client shows its own "Working" then.
 */
export function promachosStatusText(title: string | null | undefined): string | null {
  if (!title) return null;
  const plain = title.split(/\s(?=[/~`$]|[A-Za-z]:\\|https?:)/u)[0]!.trim();
  return plain.length === 0 || /^[/~`$]/u.test(plain) ? null : plain;
}
