/** Spectrum transcripts have no provider; only their hidden Drafters run turns. */
export const isSpectrumThreadId = (id: string) => id.startsWith("spectrum.");
export const isSpectrumParticipantId = (id: string) => id.startsWith("sub.spectrum.");
