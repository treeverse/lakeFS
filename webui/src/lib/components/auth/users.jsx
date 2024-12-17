import {auth, MAX_LISTING_AMOUNT} from "../../api";

export const allUsersFromLakeFS = async (resolveUserDisplayNameFN = (user => user.id)) => {
    let after = ""
    let hasMore = true
    let usersList = []
    try {
        do {
            const results = await auth.listUsers("", after, MAX_LISTING_AMOUNT);
            usersList = usersList.concat(results.results);
            after = results.pagination.next_offset;
            hasMore = results.pagination.has_more;
        } while (hasMore);
        usersList.sort((a, b) => resolveUserDisplayNameFN(a).localeCompare(resolveUserDisplayNameFN(b)));
        return usersList;
    } catch (error) {
        console.error("Error fetching users:", error);
        return [];
    }
}