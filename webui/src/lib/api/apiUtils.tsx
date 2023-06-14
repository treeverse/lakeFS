import { branches } from "./index";

export const findBranchRegexMatch = async (
  repoId: string,
  prefix: string,
  regex: string | RegExp,
  resultsPerPage = 100
): Promise<string | null> => {
  let hasMore = true;
  let after: string | undefined;
  while (hasMore) {
    const { results: branchList, pagination } = await branches.list(
      repoId,
      prefix,
      after,
      resultsPerPage
    );
    // Once we type the API we can remove the eslint-disable
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const branch = branchList.find((branch: any) => branch.id.match(regex));
    if (branch) {
      return branch.id;
    }
    hasMore = pagination.has_more;
    after = pagination.next_offset;
  }
  return null;
};
