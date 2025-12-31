import { describe, it, expect } from 'vitest';
import { mergeResults } from './mergeResults';
import { Entry, ChangesData, EntryWithDiff } from './types';

describe('mergeResults', () => {
    describe('basic behavior', () => {
        it('returns empty array when results is null or undefined', () => {
            expect(mergeResults(null, { results: [] }, false)).toEqual([]);
            expect(mergeResults(undefined, { results: [] }, false)).toEqual([]);
        });

        it('returns sorted results when changesData is null', () => {
            const results: Entry[] = [
                { path: 'file2.txt', path_type: 'object' },
                { path: 'file1.txt', path_type: 'object' },
            ];

            const merged = mergeResults(results, null, false);

            expect(merged).toEqual([
                { path: 'file1.txt', path_type: 'object' },
                { path: 'file2.txt', path_type: 'object' },
            ]);
        });

        it('returns sorted results when changesData has no results', () => {
            const results: Entry[] = [
                { path: 'file2.txt', path_type: 'object' },
                { path: 'file1.txt', path_type: 'object' },
            ];

            const merged = mergeResults(results, {} as ChangesData, false);

            expect(merged).toEqual([
                { path: 'file1.txt', path_type: 'object' },
                { path: 'file2.txt', path_type: 'object' },
            ]);
        });

        it('returns sorted results with correct diff markers based on showChangesOnly', () => {
            const results: Entry[] = [
                { path: 'file2.txt', path_type: 'object' },
                { path: 'file1.txt', path_type: 'object' },
            ];
            const changesData: ChangesData = {
                results: [{ path: 'file1.txt', type: 'changed', path_type: 'object' }],
            };

            // When showChangesOnly is true, no diff_type markers
            const mergedShowChangesOnly = mergeResults(results, changesData, true);
            expect(mergedShowChangesOnly).toEqual([
                { path: 'file1.txt', path_type: 'object' },
                { path: 'file2.txt', path_type: 'object' },
            ]);

            // When showChangesOnly is false, diff_type markers are added
            const mergedWithDiff = mergeResults(results, changesData, false);
            expect(mergedWithDiff).toEqual([
                { path: 'file1.txt', path_type: 'object', diff_type: 'changed' },
                { path: 'file2.txt', path_type: 'object' },
            ]);
        });

        it('returns empty array for empty results', () => {
            expect(mergeResults([], { results: [] }, false)).toEqual([]);
        });
    });

    describe('marking changed files', () => {
        it('marks added files with diff_type', () => {
            const results: Entry[] = [{ path: 'new-file.txt', path_type: 'object' }];
            const changesData: ChangesData = {
                results: [{ path: 'new-file.txt', type: 'added', path_type: 'object' }],
            };

            const merged = mergeResults(results, changesData, false);

            expect(merged).toEqual([{ path: 'new-file.txt', path_type: 'object', diff_type: 'added' }]);
        });

        it('marks changed files with diff_type', () => {
            const results: Entry[] = [{ path: 'modified.txt', path_type: 'object' }];
            const changesData: ChangesData = {
                results: [{ path: 'modified.txt', type: 'changed', path_type: 'object' }],
            };

            const merged = mergeResults(results, changesData, false);

            expect(merged).toEqual([{ path: 'modified.txt', path_type: 'object', diff_type: 'changed' }]);
        });

        it('maps "removed" type to "removed"', () => {
            const results: Entry[] = [{ path: 'file.txt', path_type: 'object' }];
            const changesData: ChangesData = {
                results: [{ path: 'deleted.txt', type: 'removed', path_type: 'object' }],
            };

            const merged = mergeResults(results, changesData, false);

            // The removed file should be added since it's lexicographically before file.txt
            expect(merged.find((r) => r.path === 'deleted.txt')).toMatchObject({
                path: 'deleted.txt',
                type: 'removed',
                path_type: 'object',
                diff_type: 'removed',
            });
        });

        it('does not mark unchanged files', () => {
            const results: Entry[] = [
                { path: 'unchanged.txt', path_type: 'object' },
                { path: 'changed.txt', path_type: 'object' },
            ];
            const changesData: ChangesData = {
                results: [{ path: 'changed.txt', type: 'changed', path_type: 'object' }],
            };

            const merged = mergeResults(results, changesData, false);

            expect(merged).toEqual([
                { path: 'changed.txt', path_type: 'object', diff_type: 'changed' },
                { path: 'unchanged.txt', path_type: 'object' },
            ]);
        });
    });

    describe('directory handling', () => {
        it('marks directories with direct changes', () => {
            const results: Entry[] = [{ path: 'dir/', path_type: 'common_prefix' }];
            const changesData: ChangesData = {
                results: [{ path: 'dir/', type: 'changed', path_type: 'common_prefix' }],
            };

            const merged = mergeResults(results, changesData, false);

            expect(merged).toEqual([{ path: 'dir/', path_type: 'common_prefix', diff_type: 'changed' }]);
        });

        it('marks parent directories as changed when files inside are modified', () => {
            const results: Entry[] = [
                { path: 'dir/', path_type: 'common_prefix' },
                { path: 'dir/file.txt', path_type: 'object' },
            ];
            const changesData: ChangesData = {
                results: [{ path: 'dir/file.txt', type: 'changed', path_type: 'object' }],
            };

            const merged: EntryWithDiff[] = mergeResults(results, changesData, false);

            expect(merged).toHaveLength(2);
            expect(merged.find((r) => r.path === 'dir/')).toEqual({
                path: 'dir/',
                path_type: 'common_prefix',
                diff_type: 'changed',
            });
            expect(merged.find((r) => r.path === 'dir/file.txt')).toEqual({
                path: 'dir/file.txt',
                path_type: 'object',
                diff_type: 'changed',
            });
        });

        it('marks nested parent directories as changed', () => {
            const results: Entry[] = [
                { path: 'a/', path_type: 'common_prefix' },
                { path: 'a/b/', path_type: 'common_prefix' },
                { path: 'a/b/c/', path_type: 'common_prefix' },
                { path: 'a/b/c/file.txt', path_type: 'object' },
            ];
            const changesData: ChangesData = {
                results: [{ path: 'a/b/c/file.txt', type: 'added', path_type: 'object' }],
            };

            const merged = mergeResults(results, changesData, false);

            // All directories should be marked as changed
            expect(merged.find((r) => r.path === 'a/')).toMatchObject({ diff_type: 'changed' });
            expect(merged.find((r) => r.path === 'a/b/')).toMatchObject({ diff_type: 'changed' });
            expect(merged.find((r) => r.path === 'a/b/c/')).toMatchObject({ diff_type: 'changed' });
            expect(merged.find((r) => r.path === 'a/b/c/file.txt')).toMatchObject({ diff_type: 'added' });
        });

        it('does not override direct directory change type with "changed"', () => {
            const results: Entry[] = [
                { path: 'dir/', path_type: 'common_prefix' },
                { path: 'dir/file.txt', path_type: 'object' },
            ];
            const changesData: ChangesData = {
                results: [
                    { path: 'dir/', type: 'removed', path_type: 'common_prefix' },
                    { path: 'dir/file.txt', type: 'removed', path_type: 'object' },
                ],
            };

            const merged = mergeResults(results, changesData, false);

            // Directory should keep 'removed' type, not be overridden by 'changed'
            expect(merged.find((r) => r.path === 'dir/')).toMatchObject({ diff_type: 'removed' });
        });
    });

    describe('missing items (removed files/directories)', () => {
        it('does not add removed items when results is empty', () => {
            const results: Entry[] = [];
            const changesData: ChangesData = {
                results: [{ path: 'deleted.txt', type: 'removed', path_type: 'object' }],
            };

            const merged = mergeResults(results, changesData, false);

            // When results is empty, we don't know the range, so no missing items are added
            expect(merged).toEqual([]);
        });

        it('adds removed files that are missing from results', () => {
            const results: Entry[] = [{ path: 'file1.txt', path_type: 'object' }];
            const changesData: ChangesData = {
                results: [{ path: 'deleted.txt', type: 'removed', path_type: 'object' }],
            };

            const merged = mergeResults(results, changesData, false);

            expect(merged).toHaveLength(2);
            expect(merged.find((r) => r.path === 'deleted.txt')).toMatchObject({
                path: 'deleted.txt',
                type: 'removed',
                path_type: 'object',
                diff_type: 'removed',
            });
        });

        it('adds removed common_prefix items that are missing from results', () => {
            const results: Entry[] = [{ path: 'dir1/', path_type: 'common_prefix' }];
            const changesData: ChangesData = {
                results: [{ path: 'deleted-dir/', type: 'removed', path_type: 'common_prefix' }],
            };

            const merged = mergeResults(results, changesData, false);

            expect(merged).toHaveLength(2);
            expect(merged.find((r) => r.path === 'deleted-dir/')).toMatchObject({
                path: 'deleted-dir/',
                type: 'removed',
                path_type: 'common_prefix',
                diff_type: 'removed',
            });
        });

        it('does not add removed items that come after last result path', () => {
            const results: Entry[] = [
                { path: 'a.txt', path_type: 'object' },
                { path: 'b.txt', path_type: 'object' },
            ];
            const changesData: ChangesData = {
                results: [{ path: 'z.txt', type: 'removed', path_type: 'object' }],
            };

            const merged = mergeResults(results, changesData, false);

            // z.txt should not be added because it comes after b.txt lexicographically
            expect(merged).toHaveLength(2);
            expect(merged.find((r) => r.path === 'z.txt')).toBeUndefined();
        });

        it('does not duplicate items that exist in both results and changes', () => {
            const results: Entry[] = [{ path: 'file.txt', path_type: 'object' }];
            const changesData: ChangesData = {
                results: [{ path: 'file.txt', type: 'removed', path_type: 'object' }],
            };

            const merged = mergeResults(results, changesData, false);

            expect(merged).toHaveLength(1);
            expect(merged[0]).toMatchObject({
                path: 'file.txt',
                diff_type: 'removed',
            });
        });

        it('adds common_prefix changes within the results range', () => {
            const results: Entry[] = [
                { path: 'a/', path_type: 'common_prefix' },
                { path: 'z/', path_type: 'common_prefix' },
            ];
            const changesData: ChangesData = {
                results: [{ path: 'deleted-dir/', type: 'changed', path_type: 'common_prefix' }],
            };

            const merged = mergeResults(results, changesData, false);

            // deleted-dir/ is between a/ and z/ lexicographically, so it should be added
            expect(merged).toHaveLength(3);
            expect(merged.find((r) => r.path === 'deleted-dir/')).toMatchObject({
                path: 'deleted-dir/',
                type: 'changed',
                path_type: 'common_prefix',
                diff_type: 'changed',
            });
        });
    });

    describe('sorting', () => {
        it('sorts merged results lexicographically', () => {
            const results: Entry[] = [
                { path: 'c.txt', path_type: 'object' },
                { path: 'a.txt', path_type: 'object' },
                { path: 'd.txt', path_type: 'object' },
            ];
            const changesData: ChangesData = {
                results: [
                    { path: 'b.txt', type: 'removed', path_type: 'object' },
                    { path: 'a.txt', type: 'changed', path_type: 'object' },
                ],
            };

            const merged = mergeResults(results, changesData, false);

            // b.txt is removed and within the range (before d.txt), so it should be included
            expect(merged.map((r) => r.path)).toEqual(['a.txt', 'b.txt', 'c.txt', 'd.txt']);
        });

        it('sorts with directories and files mixed', () => {
            const results: Entry[] = [
                { path: 'z-file.txt', path_type: 'object' },
                { path: 'a-dir/', path_type: 'common_prefix' },
                { path: 'm-file.txt', path_type: 'object' },
            ];
            const changesData: ChangesData = {
                results: [{ path: 'a-dir/', type: 'changed', path_type: 'common_prefix' }],
            };

            const merged = mergeResults(results, changesData, false);

            expect(merged.map((r) => r.path)).toEqual(['a-dir/', 'm-file.txt', 'z-file.txt']);
        });
    });

    describe('mixed-type scenarios (file <-> directory)', () => {
        it('handles file replaced by directory with subpaths', () => {
            // Before: a/b/c (file/object)
            // After:  a/b/c/ (directory)
            //         a/b/c/d (file)
            //         a/b/c/e (file)
            const results: Entry[] = [
                { path: 'a/b/c/', path_type: 'common_prefix' },
                { path: 'a/b/c/d', path_type: 'object' },
                { path: 'a/b/c/e', path_type: 'object' },
            ];
            const changesData: ChangesData = {
                results: [
                    { path: 'a/b/c', type: 'removed', path_type: 'object' }, // Old file removed
                    { path: 'a/b/c/', type: 'added', path_type: 'common_prefix' }, // New directory added
                    { path: 'a/b/c/d', type: 'added', path_type: 'object' },
                    { path: 'a/b/c/e', type: 'added', path_type: 'object' },
                ],
            };

            const merged = mergeResults(results, changesData, false);

            expect(merged).toEqual([
                { path: 'a/b/c', path_type: 'object', type: 'removed', diff_type: 'removed' },
                { path: 'a/b/c/', path_type: 'common_prefix', diff_type: 'added' },
                { path: 'a/b/c/d', path_type: 'object', diff_type: 'added' },
                { path: 'a/b/c/e', path_type: 'object', diff_type: 'added' },
            ]);
        });

        it('handles file with subpaths added below it', () => {
            // Before: a/b/c (file/object)
            // After:  a/b/c (file/object - still exists)
            //         a/b/c/d (file)
            //         a/b/c/e (file)
            const results: Entry[] = [
                { path: 'a/b/c', path_type: 'object' },
                { path: 'a/b/c/d', path_type: 'object' },
                { path: 'a/b/c/e', path_type: 'object' },
            ];
            const changesData: ChangesData = {
                results: [
                    { path: 'a/b/c/d', type: 'added', path_type: 'object' },
                    { path: 'a/b/c/e', type: 'added', path_type: 'object' },
                ],
            };

            const merged = mergeResults(results, changesData, false);

            expect(merged).toEqual([
                { path: 'a/b/c', path_type: 'object' },
                { path: 'a/b/c/d', path_type: 'object', diff_type: 'added' },
                { path: 'a/b/c/e', path_type: 'object', diff_type: 'added' },
            ]);
        });

        it('handles directory replaced by file', () => {
            // Before: a/b/c/ (directory)
            //         a/b/c/d (file)
            //         a/b/c/e (file)
            // After:  a/b/c (file/object)
            //         a/b/d (some other file to establish the range)
            const results: Entry[] = [
                { path: 'a/b/c', path_type: 'object' },
                { path: 'a/b/d', path_type: 'object' },
            ];
            const changesData: ChangesData = {
                results: [
                    { path: 'a/b/c/', type: 'removed', path_type: 'common_prefix' }, // Directory removed
                    { path: 'a/b/c/d', type: 'removed', path_type: 'object' },
                    { path: 'a/b/c/e', type: 'removed', path_type: 'object' },
                    { path: 'a/b/c', type: 'added', path_type: 'object' }, // File added in its place
                ],
            };

            const merged = mergeResults(results, changesData, false);

            expect(merged).toEqual([
                { path: 'a/b/c', path_type: 'object', diff_type: 'added' },
                { path: 'a/b/c/', path_type: 'common_prefix', type: 'removed', diff_type: 'removed' },
                { path: 'a/b/c/d', path_type: 'object', type: 'removed', diff_type: 'removed' },
                { path: 'a/b/c/e', path_type: 'object', type: 'removed', diff_type: 'removed' },
                { path: 'a/b/d', path_type: 'object' },
            ]);
        });

        it('handles file and directory with same prefix coexisting', () => {
            // Both a/b/c (file) and a/b/c/d (implying a/b/c/ directory) exist
            const results: Entry[] = [
                { path: 'a/b/c', path_type: 'object' },
                { path: 'a/b/c/', path_type: 'common_prefix' },
                { path: 'a/b/c/d', path_type: 'object' },
            ];
            const changesData: ChangesData = {
                results: [
                    { path: 'a/b/c', type: 'changed', path_type: 'object' },
                    { path: 'a/b/c/d', type: 'added', path_type: 'object' },
                ],
            };

            const merged = mergeResults(results, changesData, false);

            expect(merged).toEqual([
                { path: 'a/b/c', path_type: 'object', diff_type: 'changed' },
                { path: 'a/b/c/', path_type: 'common_prefix', diff_type: 'changed' },
                { path: 'a/b/c/d', path_type: 'object', diff_type: 'added' },
            ]);
        });

        it('sorts correctly when file and directory with same prefix have mixed changes', () => {
            const results: Entry[] = [
                { path: 'a/b/c', path_type: 'object' },
                { path: 'a/b/c/', path_type: 'common_prefix' },
                { path: 'a/b/c/d', path_type: 'object' },
                { path: 'a/b/c/e', path_type: 'object' },
                { path: 'a/b/d', path_type: 'object' },
            ];
            const changesData: ChangesData = {
                results: [
                    { path: 'a/b/c', type: 'removed', path_type: 'object' },
                    { path: 'a/b/c/d', type: 'added', path_type: 'object' },
                ],
            };

            const merged = mergeResults(results, changesData, false);

            expect(merged).toEqual([
                { path: 'a/b/c', path_type: 'object', diff_type: 'removed' },
                { path: 'a/b/c/', path_type: 'common_prefix', diff_type: 'changed' },
                { path: 'a/b/c/d', path_type: 'object', diff_type: 'added' },
                { path: 'a/b/c/e', path_type: 'object' },
                { path: 'a/b/d', path_type: 'object' },
            ]);
        });
    });

    describe('complex scenarios', () => {
        it('handles multiple changes of different types', () => {
            const results: Entry[] = [
                { path: 'added.txt', path_type: 'object' }, // Added files are in results
                { path: 'existing.txt', path_type: 'object' },
                { path: 'modified.txt', path_type: 'object' },
                { path: 'unchanged.txt', path_type: 'object' },
            ];
            const changesData: ChangesData = {
                results: [
                    { path: 'added.txt', type: 'added', path_type: 'object' },
                    { path: 'deleted.txt', type: 'removed', path_type: 'object' },
                    { path: 'modified.txt', type: 'changed', path_type: 'object' },
                ],
            };

            const merged = mergeResults(results, changesData, false);

            // deleted.txt is within range (before unchanged.txt) so should be added
            expect(merged).toHaveLength(5);
            expect(merged.find((r) => r.path === 'added.txt')).toMatchObject({ diff_type: 'added' });
            expect(merged.find((r) => r.path === 'deleted.txt')).toMatchObject({ diff_type: 'removed' });
            expect(merged.find((r) => r.path === 'modified.txt')).toMatchObject({ diff_type: 'changed' });
            expect(merged.find((r) => r.path === 'unchanged.txt')).not.toHaveProperty('diff_type');
        });

        it('handles nested directory structures with changes at multiple levels', () => {
            const results: Entry[] = [
                { path: 'root/', path_type: 'common_prefix' },
                { path: 'root/level1/', path_type: 'common_prefix' },
                { path: 'root/level1/file1.txt', path_type: 'object' },
                { path: 'root/level1/file2.txt', path_type: 'object' },
                { path: 'root/other/', path_type: 'common_prefix' },
            ];
            const changesData: ChangesData = {
                results: [
                    { path: 'root/level1/file1.txt', type: 'changed', path_type: 'object' },
                    { path: 'root/level1/file2.txt', type: 'added', path_type: 'object' },
                ],
            };

            const merged = mergeResults(results, changesData, false);

            // root/ and root/level1/ should be marked as changed
            expect(merged.find((r) => r.path === 'root/')).toMatchObject({ diff_type: 'changed' });
            expect(merged.find((r) => r.path === 'root/level1/')).toMatchObject({ diff_type: 'changed' });
            // root/other/ should not have diff_type
            expect(merged.find((r) => r.path === 'root/other/')).not.toHaveProperty('diff_type');
            // Files should have their respective types
            expect(merged.find((r) => r.path === 'root/level1/file1.txt')).toMatchObject({ diff_type: 'changed' });
            expect(merged.find((r) => r.path === 'root/level1/file2.txt')).toMatchObject({ diff_type: 'added' });
        });

        it('preserves all entry properties while adding diff_type', () => {
            const results: Entry[] = [
                {
                    path: 'file.txt',
                    path_type: 'object',
                    physical_address: 's3://bucket/file.txt',
                    size: 1024,
                    mtime: 1234567890,
                },
            ];
            const changesData: ChangesData = {
                results: [{ path: 'file.txt', type: 'changed', path_type: 'object' }],
            };

            const merged = mergeResults(results, changesData, false);

            expect(merged[0]).toEqual({
                path: 'file.txt',
                path_type: 'object',
                physical_address: 's3://bucket/file.txt',
                size: 1024,
                mtime: 1234567890,
                diff_type: 'changed',
            });
        });
    });
});
