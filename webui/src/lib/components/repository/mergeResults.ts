import { last } from 'lodash';
import { ChangesData, DiffType, Entry, EntryWithDiff } from './types';
import { compareLexicographically } from '../../utils';

/**
 * Merges object listing results with changes data to highlight modified/added/removed items.
 *
 * @param results - Array of object entries from the main listing
 * @param changesData - Changes data containing results array with change information
 * @param showChangesOnly - Whether to show only changes (if true, no merging needed)
 * @param hasMore - Whether there are more pages of results after the current one
 * @returns Merged and sorted results with diff_type annotations
 */
export function mergeResults(
    results: Entry[] | undefined | null,
    changesData: ChangesData | undefined | null,
    showChangesOnly = false,
    hasMore = false,
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

    // Add missing items for removed entries, added entries, and prefixes
    // When using committed-only ref (branch@) for objects.list, added entries are not in the list
    // On paginated results, only add items within the current page range to avoid duplicates
    const lastResultPath = last(results)?.path;
    const inPageRange = (path: string) => lastResultPath && path <= lastResultPath;
    const missingItems = changesData.results
        .filter(
            (change) => change.type === 'removed' || change.type === 'added' || change.path_type === 'common_prefix',
        )
        .filter((change) => {
            if (change.type === 'added') {
                // Added entries never appear in committed results (branch@),
                // so include them on the last page even if beyond lastResultPath
                return inPageRange(change.path) || !hasMore;
            }
            // Removed/changed entries exist in committed results on some page,
            // only show within the current page range
            return inPageRange(change.path);
        })
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
