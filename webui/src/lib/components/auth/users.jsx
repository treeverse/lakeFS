import { auth, MAX_LISTING_AMOUNT } from "../../api";

export const allUsersFromLakeFS = async (resolveUserDisplayNameFN = (user => user.id)) => {
    const fetchUsers = async (after = "", usersList = []) => {
        try {
            const results = await auth.listUsers("", after, MAX_LISTING_AMOUNT);
            const newUsersList = usersList.concat(results.results);

            if (results.pagination.has_more) {
                return fetchUsers(results.pagination.next_offset, newUsersList);
            }

            newUsersList.sort((a, b) => resolveUserDisplayNameFN(a).localeCompare(resolveUserDisplayNameFN(b)));
            return newUsersList;
        } catch (error) {
            console.error("Error fetching users:", error);
            return [];
        }
    };

    return fetchUsers();
}