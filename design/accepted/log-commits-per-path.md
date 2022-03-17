## Log Commits Per Path

### Problem Description

This feature gives the ability to log commits that contain changes only to a specified path or list of paths.
The path can represent a specific object or a prefix.

For example: `lakectl log lakefs://example1/main/path/to/object` will output only the commits in which the object changed.

You can see the relevant issue [here](https://github.com/treeverse/lakeFS/issues/2251).

### How does it work in git?

The equivalent git command is [git log](https://git-scm.com/docs/git-log).

In a similar way, `git log path/to/object.txt` will output only the commits in which the object changed.

Git traverses over the commits object and does the following:
Goes over the commit tree and searches for the specified path. If it exists in the tree - checks if it's with the same SHA1 like the parent commit, and if no - outputs it.

### How does it work with lakeFS?

We implemented the feature in a way similar to git. The feature uses the existing implementation of the log api.
The log function goes over the commits tree, starting with a branch or commit reference, and outputs each commit in the tree.
We added a simple check - if there are paths specified, we output the commit only if it contains changes to the path (or list of paths).
We answer this by this simple mechanism:
1. We get the diff iterator between the commit and its parent.
2. We jump into the specified path (by SeekGE), and check if the path is in the diff. If so, we output the commit.

This mechanism works because if a path changed in a commit - it means that the path should be part of the diff between the commit and its parent, and thus should be found by the diff iterator.
We search the path in the diff iterator with SeekGE, which returns the first result that is equal or greater than the search key.
Therefore, if the path is an object that changed in that commit - then the diff iterator should jump exactly to that object.
In the same way, if the path is a prefix that some object in it changed in that commit - then the diff iterator should jump to the first object that changed, and its path should contain the prefix.
SeekGe gives us the ability to jump to the requested path and not go over the all diff.

If we have a list of paths we search each path in the diff until we find one of them. That means that we list the commits that contains changes to at least one path from the list.

Another important point is that we do this process only to commits that has a single parent - that is, not merge commits. If a commit has one parent, then we diff it commit with the parent.
The merge commits were not included to align with git's behavior.

### Changes done to the api and lakectl

We added two parameters to the log api: a prefixes list and an objects list.
Accordingly, we added two flags to the lakectl log command - prefixes and objects. Both flags can accept multiple values that creates the relevant lists.
If both objects list and prefixes list are empty - then we log all commits.

### Future possible updates

As in git, we don't consider merge commits. We can add a `--first-parent` flag to tell `log` to ignore the second (and any additional) parent of a merge,
which leaves it with just one parent. This means we won't follow the side branch at all.

### references

1. https://stackoverflow.com/questions/37801342/using-git-log-to-display-files-changed-during-merge
2. https://stackoverflow.com/questions/21178383/how-is-gits-log-filename-implemented-internally