export const MAX_UNTRIMMED_RESULT_LENGTH = 50;

/**
 * Returns display name for a ref, replacing long prefixes with '...' for readability.
 * Used when filtering refs by prefix - if the prefix matches the start of a long ref name,
 * we show '...' followed by the remainder to save space while keeping context.
 */
export const getRefDisplayName = (refId: string, replacePrefix?: string): string => {
    if (replacePrefix && refId !== replacePrefix && refId.startsWith(replacePrefix)) {
        return '...' + refId.slice(replacePrefix.length);
    }
    return refId;
};
