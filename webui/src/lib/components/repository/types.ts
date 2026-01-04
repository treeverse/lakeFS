/**
 * Type for change/diff indicators
 */
export type DiffType = 'added' | 'changed' | 'removed';

/**
 * Type for path types in the tree
 */
export type PathType = 'object' | 'common_prefix';

/**
 * Base entry structure for objects and directories
 */
export interface Entry {
    path: string;
    path_type: PathType;
    [key: string]: unknown; // Allow additional properties
}

/**
 * Entry with diff type annotation
 */
export interface EntryWithDiff extends Entry {
    diff_type?: DiffType;
}

/**
 * Change entry from the changes API
 */
export interface ChangeEntry extends Entry {
    type: 'added' | 'changed' | 'removed';
}

/**
 * Changes data structure from the API
 */
export interface ChangesData {
    results: ChangeEntry[];
    [key: string]: unknown; // Allow additional properties like pagination
}
