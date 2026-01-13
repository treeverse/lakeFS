import { last } from 'lodash';
import { ChangesData, DiffType, Entry, EntryWithDiff } from './types';
import { compareLexicographically } from '../../utils';

/**
 * Merges object listing results with changes data to highlight modified/added/removed items.
 *
 * @param results - Array of object entries from the main listing
 * @param changesData - Changes data containing results array with change information
 * @param showChangesOnly - Whether to show only changes (if true, no merging needed)
 * @returns Merged and sorted results with diff_type annotations
 */
export function mergeResults(
    results: Entry[] | undefined | null,
    changesData: ChangesData | undefined | null,
    showChangesOnly = false,
): EntryWithDiff[] {
    if (showChangesOnly || !results || !changesData?.results) {
        // Ensure regular results are also sorted lexicographically
        return results?.sort((a, b) => compareLexicographically(a.path, b.path)) ?? [];
    }

    const changesMap = new Map<string, DiffType>();
    const directoryChanges = new Map<string, DiffType>(); // Store change type for directories

    // Map direct changes and identify affected directories
    changesData.results.forEach((change) => {
        // Map change type to what EntryRow expects
        const mappedType = change.type === 'removed' ? 'removed' : change.type === 'added' ? 'added' : 'changed';

        changesMap.set(change.path, mappedType);

        // If this is a prefix entry from changes, mark it directly with its type
        if (change.path_type === 'common_prefix') {
            directoryChanges.set(change.path, mappedType);
        } else {
            // For file changes, mark parent directories as changed
            const pathParts = change.path.split('/');
            for (let i = 1; i < pathParts.length; i++) {
                const dirPath = pathParts.slice(0, i).join('/') + '/';
                // Only mark as changed if not already marked with a more specific type
                if (!directoryChanges.has(dirPath)) {
                    directoryChanges.set(dirPath, 'changed');
                }
            }
        }
    });

    // Add missing items only for removed entries or deleted prefixes
    // Avoid adding items that come after the last result path (both are sorted lexicographically)
    const lastResultPath = last(results)?.path;
    const missingItems = changesData.results
        .filter((change) => change.type === 'removed' || change.path_type === 'common_prefix')
        .filter((change) => lastResultPath && change.path <= lastResultPath)
        .filter((change) => !results.find((result) => result.path === change.path));

    // Merge regular results with change info
    const enhancedResults = results.map((entry) => {
        const directChangeType = changesMap.get(entry.path);
        if (directChangeType) {
            return { ...entry, diff_type: directChangeType };
        }

        // Check if this directory contains changes (either directly or has changed children)
        const directoryChangeType = directoryChanges.get(entry.path);
        if (entry.path_type === 'common_prefix' && directoryChangeType) {
            return { ...entry, diff_type: directoryChangeType };
        }

        return entry;
    });

    // Add missing items (deleted files, deleted/changed prefixes) to the results
    const allResults: EntryWithDiff[] = [
        ...enhancedResults,
        ...missingItems.map((item): EntryWithDiff => {
            const mappedType: DiffType =
                item.type === 'removed' ? 'removed' : item.type === 'added' ? 'added' : 'changed';
            return {
                ...item,
                diff_type: mappedType,
            };
        }),
    ];

    // Sort to maintain proper order
    return allResults?.sort((a, b) => compareLexicographically(a.path, b.path)) ?? [];
}
