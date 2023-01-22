export const RefTypeBranch = 'branch';
export const RefTypeCommit = 'commit';
export const RefTypeTag = 'tag';
export enum TreeItemType {
    Object,
    Prefix,
    DeltaLakeTable
}

export enum DiffType {
    Changed = "changed",
    Added = "added",
    Removed = "removed",
    Conflict = "conflict"
}
