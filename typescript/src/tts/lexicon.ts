import { RimeInputError } from "../errors.js";

/** A word or phrase and its space-separated X-SAMPA pronunciation. */
export interface PronunciationEntry {
  readonly spelling: string;
  readonly pronunciation: string;
}

/** Copy request options now; model and linguistic validation belong to the service. */
export function snapshot(
  entries: readonly PronunciationEntry[] = [],
): readonly PronunciationEntry[] {
  if (!Array.isArray(entries))
    throw new RimeInputError(
      "customLexicon must be an array of pronunciation entries",
    );
  // Array.from also visits holes, which are invalid entries.
  return Object.freeze(
    Array.from(entries, (entry) => {
      if (
        !entry ||
        typeof entry !== "object" ||
        typeof entry.spelling !== "string" ||
        typeof entry.pronunciation !== "string"
      )
        throw new RimeInputError(
          "Lexicon entries must have string spelling and pronunciation fields",
        );
      return Object.freeze({
        spelling: entry.spelling,
        pronunciation: entry.pronunciation,
      });
    }),
  );
}
