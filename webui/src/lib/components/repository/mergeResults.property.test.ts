import { describe, it, expect } from 'vitest';
import * as fc from 'fast-check';
import { mergeResults } from './mergeResults';
import { Entry, ChangesData, DiffType } from './types';

/**
 * Property-based tests for mergeResults function using fast-check.
 *
 * These tests complement the example-based tests by verifying invariants
 * across thousands of randomly generated inputs to catch edge cases.
 */

// ==================== Arbitraries (Random Data Generators) ====================

/**
 * Generates valid path segments (alphanumeric + common chars)
 */
const pathSegmentArbitrary = fc
    .array(fc.constantFrom('a', 'b', 'c', 'd', 'e', 'f', 'g', '1', '2', '3', '0', '_', '-'), {
        minLength: 1,
        maxLength: 15,
    })
    .map((chars: string[]) => chars.join(''));

/**
 * Generates file paths (no trailing slash) like "dir1/dir2/file.txt"
 */
const filePathArbitrary = fc
    .array(pathSegmentArbitrary, { minLength: 1, maxLength: 4 })
    .map((parts) => parts.join('/'));

/**
 * Generates directory paths (with trailing slash) like "dir1/dir2/"
 */
const dirPathArbitrary = fc
    .array(pathSegmentArbitrary, { minLength: 1, maxLength: 4 })
    .map((parts) => parts.join('/') + '/');

/**
 * Generates either file or directory path
 */
const anyPathArbitrary = fc.oneof(filePathArbitrary, dirPathArbitrary);

/**
 * Generates a complete Entry object
 */
const entryArbitrary: fc.Arbitrary<Entry> = fc
    .tuple(anyPathArbitrary, fc.nat(), fc.nat())
    .map(([path, size, mtime]: [string, number, number]) => {
        // Ensure path_type matches path format
        const actualPathType = path.endsWith('/') ? 'common_prefix' : 'object';
        return {
            path: path,
            path_type: actualPathType,
            physical_address: `s3://bucket/${path}`,
            size: actualPathType === 'object' ? size : undefined,
            mtime: mtime,
        };
    });

/**
 * Generates a change entry (used in ChangesData.results)
 */
const changeEntryArbitrary = fc.tuple(anyPathArbitrary, fc.constantFrom('added', 'removed', 'changed')).map(
    ([path, type]: [string, 'added' | 'removed' | 'changed']): {
        path: string;
        type: 'added' | 'removed' | 'changed';
        path_type: 'object' | 'common_prefix';
    } => {
        const path_type = path.endsWith('/') ? 'common_prefix' : 'object';
        return {
            path,
            type,
            path_type,
        };
    },
);

/**
 * Generates an array of unique entries (no duplicate paths)
 */
const uniqueEntriesArbitrary = fc.array(entryArbitrary, { minLength: 0, maxLength: 20 }).map((entries) => {
    // Remove duplicates by path
    const seen = new Set<string>();
    return entries.filter((entry) => {
        if (seen.has(entry.path)) return false;
        seen.add(entry.path);
        return true;
    });
});

/**
 * Generates an array of unique change entries
 */
const uniqueChangesArbitrary = fc.array(changeEntryArbitrary, { minLength: 0, maxLength: 20 }).map((changes) => {
    const seen = new Set<string>();
    return changes.filter((change) => {
        if (seen.has(change.path)) return false;
        seen.add(change.path);
        return true;
    });
});

// ==================== Property Tests ====================

describe('mergeResults - Property-Based Tests', () => {
    describe('Simple Invariants', () => {
        it('PROPERTY: output is always sorted lexicographically', () => {
            fc.assert(
                fc.property(uniqueEntriesArbitrary, uniqueChangesArbitrary, (results, changes) => {
                    const changesData: ChangesData = { results: changes };
                    const merged = mergeResults(results, changesData, false);

                    // Verify sorting
                    for (let i = 0; i < merged.length - 1; i++) {
                        const comparison = merged[i].path.localeCompare(merged[i + 1].path);
                        expect(comparison).toBeLessThanOrEqual(0);
                    }
                }),
                { numRuns: 100 },
            );
        });

        it('PROPERTY: no duplicate paths in output', () => {
            fc.assert(
                fc.property(uniqueEntriesArbitrary, uniqueChangesArbitrary, (results, changes) => {
                    const changesData: ChangesData = { results: changes };
                    const merged = mergeResults(results, changesData, false);

                    const paths = merged.map((e) => e.path);
                    const uniquePaths = new Set(paths);

                    expect(paths.length).toBe(uniquePaths.size);
                }),
                { numRuns: 100 },
            );
        });

        it('PROPERTY: all original results are preserved in output', () => {
            fc.assert(
                fc.property(uniqueEntriesArbitrary, uniqueChangesArbitrary, (results, changes) => {
                    const changesData: ChangesData = { results: changes };
                    const merged = mergeResults(results, changesData, false);

                    const mergedPaths = new Set(merged.map((e) => e.path));

                    // Every result should appear in merged output
                    results.forEach((entry) => {
                        expect(mergedPaths.has(entry.path)).toBe(true);
                    });
                }),
                { numRuns: 100 },
            );
        });

        it('PROPERTY: output length is at least as large as input results', () => {
            fc.assert(
                fc.property(uniqueEntriesArbitrary, uniqueChangesArbitrary, (results, changes) => {
                    const changesData: ChangesData = { results: changes };
                    const merged = mergeResults(results, changesData, false);

                    // Merged results should contain at least all original results
                    expect(merged.length).toBeGreaterThanOrEqual(results.length);
                }),
                { numRuns: 100 },
            );
        });

        it('PROPERTY: properties are preserved when entries are merged', () => {
            fc.assert(
                fc.property(uniqueEntriesArbitrary, uniqueChangesArbitrary, (results, changes) => {
                    const changesData: ChangesData = { results: changes };
                    const merged = mergeResults(results, changesData, false);

                    // For each original result, verify its properties are preserved
                    results.forEach((originalEntry) => {
                        const mergedEntry = merged.find((e) => e.path === originalEntry.path);
                        expect(mergedEntry).toBeDefined();
                        if (mergedEntry) {
                            expect(mergedEntry.path_type).toBe(originalEntry.path_type);
                            expect(mergedEntry.size).toBe(originalEntry.size);
                            expect(mergedEntry.mtime).toBe(originalEntry.mtime);
                            expect(mergedEntry.physical_address).toBe(originalEntry.physical_address);
                        }
                    });
                }),
                { numRuns: 100 },
            );
        });
    });

    describe('Diff Type Invariants', () => {
        it('PROPERTY: only entries in changesData (or parent dirs) have diff_type', () => {
            fc.assert(
                fc.property(uniqueEntriesArbitrary, uniqueChangesArbitrary, (results, changes) => {
                    const changesData: ChangesData = { results: changes };
                    const merged = mergeResults(results, changesData, false);

                    const changedPaths = new Set(changes.map((c) => c.path));

                    merged.forEach((entry) => {
                        if (entry.diff_type) {
                            // Entry must either be directly in changes, or be a parent directory
                            const isDirectChange = changedPaths.has(entry.path);
                            const isParentDir =
                                entry.path_type === 'common_prefix' &&
                                Array.from(changedPaths).some((changedPath) => changedPath.startsWith(entry.path));

                            expect(isDirectChange || isParentDir).toBe(true);
                        }
                    });
                }),
                { numRuns: 100 },
            );
        });

        it('PROPERTY: all entries in changesData have diff_type in output (if within range)', () => {
            fc.assert(
                fc.property(uniqueEntriesArbitrary, uniqueChangesArbitrary, (results, changes) => {
                    if (results.length === 0) return; // Skip if no results

                    const changesData: ChangesData = { results: changes };
                    const merged = mergeResults(results, changesData, false);

                    // Sort results to find the last path
                    const sortedResults = [...results].sort((a, b) => a.path.localeCompare(b.path));
                    const lastPath = sortedResults[sortedResults.length - 1].path;

                    const mergedMap = new Map(merged.map((e) => [e.path, e]));

                    changes.forEach((change) => {
                        // Only check changes within the range
                        if (change.path <= lastPath || change.type !== 'removed') {
                            const mergedEntry = mergedMap.get(change.path);
                            if (mergedEntry) {
                                expect(mergedEntry.diff_type).toBeDefined();
                            }
                        }
                    });
                }),
                { numRuns: 100 },
            );
        });

        it('PROPERTY: diff_type values are valid', () => {
            fc.assert(
                fc.property(uniqueEntriesArbitrary, uniqueChangesArbitrary, (results, changes) => {
                    const changesData: ChangesData = { results: changes };
                    const merged = mergeResults(results, changesData, false);

                    const validDiffTypes: DiffType[] = ['added', 'removed', 'changed'];

                    merged.forEach((entry) => {
                        if (entry.diff_type) {
                            expect(validDiffTypes).toContain(entry.diff_type);
                        }
                    });
                }),
                { numRuns: 100 },
            );
        });
    });

    describe('Range Constraint Invariants', () => {
        it('PROPERTY: removed items beyond last result path are not added', () => {
            fc.assert(
                fc.property(uniqueEntriesArbitrary, uniqueChangesArbitrary, (results, changes) => {
                    if (results.length === 0) return; // Skip if no results

                    const changesData: ChangesData = { results: changes };
                    const merged = mergeResults(results, changesData, false);

                    // Find the last path in results
                    const sortedResults = [...results].sort((a, b) => a.path.localeCompare(b.path));
                    const lastPath = sortedResults[sortedResults.length - 1].path;

                    const mergedPaths = new Set(merged.map((e) => e.path));

                    // Removed items beyond lastPath should not be in merged results
                    changes.forEach((change) => {
                        if (change.type === 'removed' && change.path > lastPath) {
                            // This path should not be in merged results unless it was in original results
                            const wasInOriginalResults = results.some((r) => r.path === change.path);
                            if (!wasInOriginalResults) {
                                expect(mergedPaths.has(change.path)).toBe(false);
                            }
                        }
                    });
                }),
                { numRuns: 100 },
            );
        });
    });

    describe('Directory Propagation Invariants', () => {
        it('PROPERTY: when a file changes, parent directories are marked as changed', () => {
            fc.assert(
                fc.property(uniqueEntriesArbitrary, uniqueChangesArbitrary, (results, changes) => {
                    const changesData: ChangesData = { results: changes };
                    const merged = mergeResults(results, changesData, false);

                    const mergedMap = new Map(merged.map((e) => [e.path, e]));

                    // For each changed file (not directory), check parent directories
                    changes.forEach((change) => {
                        if (change.path_type === 'object') {
                            const pathParts = change.path.split('/');

                            // Check each parent directory
                            for (let i = 1; i < pathParts.length; i++) {
                                const parentDirPath = pathParts.slice(0, i).join('/') + '/';
                                const parentEntry = mergedMap.get(parentDirPath);

                                // If the parent dir exists in results, it should have diff_type
                                if (parentEntry) {
                                    expect(parentEntry.diff_type).toBeDefined();
                                }
                            }
                        }
                    });
                }),
                { numRuns: 100 },
            );
        });
    });

    describe('Edge Cases', () => {
        it('PROPERTY: handles empty results gracefully', () => {
            fc.assert(
                fc.property(uniqueChangesArbitrary, (changes) => {
                    const changesData: ChangesData = { results: changes };
                    const merged = mergeResults([], changesData, false);

                    // Should return empty array
                    expect(merged).toEqual([]);
                }),
                { numRuns: 100 },
            );
        });

        it('PROPERTY: handles null/undefined results gracefully', () => {
            fc.assert(
                fc.property(uniqueChangesArbitrary, (changes) => {
                    const changesData: ChangesData = { results: changes };
                    const mergedNull = mergeResults(null, changesData, false);
                    const mergedUndefined = mergeResults(undefined, changesData, false);

                    expect(mergedNull).toEqual([]);
                    expect(mergedUndefined).toEqual([]);
                }),
                { numRuns: 100 },
            );
        });

        it('PROPERTY: handles empty changes gracefully', () => {
            fc.assert(
                fc.property(uniqueEntriesArbitrary, (results) => {
                    const changesData: ChangesData = { results: [] };
                    const merged = mergeResults(results, changesData, false);

                    // Should return sorted results without diff_type
                    const sortedResults = [...results].sort((a, b) => a.path.localeCompare(b.path));
                    expect(merged.length).toBe(sortedResults.length);
                    expect(merged.every((e) => !e.diff_type)).toBe(true);
                }),
                { numRuns: 100 },
            );
        });

        it('PROPERTY: showChangesOnly=true returns sorted results without merging', () => {
            fc.assert(
                fc.property(uniqueEntriesArbitrary, uniqueChangesArbitrary, (results, changes) => {
                    const changesData: ChangesData = { results: changes };
                    const merged = mergeResults(results, changesData, true);

                    // Should return sorted results without any diff_type
                    const sortedResults = [...results].sort((a, b) => a.path.localeCompare(b.path));
                    expect(merged.length).toBe(sortedResults.length);
                    expect(merged.map((e) => e.path)).toEqual(sortedResults.map((e) => e.path));
                }),
                { numRuns: 100 },
            );
        });
    });

    describe('Idempotence and Determinism', () => {
        it('PROPERTY: running mergeResults twice with same inputs produces same output', () => {
            fc.assert(
                fc.property(uniqueEntriesArbitrary, uniqueChangesArbitrary, (results, changes) => {
                    const changesData: ChangesData = { results: changes };

                    const merged1 = mergeResults(results, changesData, false);
                    const merged2 = mergeResults(results, changesData, false);

                    expect(merged1).toEqual(merged2);
                }),
                { numRuns: 100 },
            );
        });

        it('PROPERTY: mergeResults does not mutate input arrays', () => {
            fc.assert(
                fc.property(uniqueEntriesArbitrary, uniqueChangesArbitrary, (results, changes) => {
                    const resultsCopy = JSON.parse(JSON.stringify(results));
                    const changesCopy = JSON.parse(JSON.stringify(changes));

                    const changesData: ChangesData = { results: changes };
                    mergeResults(results, changesData, false);

                    // Verify inputs are not mutated
                    expect(results).toEqual(resultsCopy);
                    expect(changes).toEqual(changesCopy);
                }),
                { numRuns: 100 },
            );
        });
    });
});
