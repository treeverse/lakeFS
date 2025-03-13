// import React, {createContext, useCallback, useEffect, useMemo, useState} from "react";
// import {Outlet, useOutletContext} from "react-router-dom";
//
// import Button from "react-bootstrap/Button";
// import Form from "react-bootstrap/Form";
// import InputGroup from "react-bootstrap/InputGroup";
// import {SearchIcon} from "@primer/octicons-react";
//
// import {useAPI} from "../../../lib/hooks/api";
// import {auth} from "../../../lib/api";
// import useUser from "../../../lib/hooks/user";
// import {ConfirmationButton} from "../../../lib/components/modals";
// import {EntityActionModal} from "../../../lib/components/auth/forms";
// import {Paginator} from "../../../lib/components/pagination";
// import {Link} from "../../../lib/components/nav";
// import {
//     ActionGroup,
//     ActionsBar,
//     AlertError,
//     Checkbox,
//     DataTable,
//     FormattedDate,
//     Loading,
//     RefreshButton,
//     useDebouncedState
// } from "../../../lib/components/controls";
// import validator from "validator/es";
// import {disallowPercentSign, INVALID_USER_NAME_ERROR_MESSAGE} from "../validation";
// import {resolveUserDisplayName} from "../../../lib/utils";
// import {allUsersFromLakeFS} from "../../../lib/components/auth/users";
// import {useRouter} from "../../../lib/hooks/router";
//
// const DEFAULT_LISTING_AMOUNT = 10;
// const DECIMAL_RADIX = 10;
// const USER_NOT_FOUND = "unknown";
// export const GetUserEmailByIdContext = createContext();
//
//
// const UsersContainer = ({ refresh, setRefresh, allUsers, loading, error }) => {
//     const { user } = useUser();
//     const currentUser = user;
//
//     const router = useRouter();
//     const [selected, setSelected] = useState([]);
//     const [deleteError, setDeleteError] = useState(null);
//     const [showCreate, setShowCreate] = useState(false);
//     const [showInvite, setShowInvite] = useState(false);
//
//     const routerUserIdxFromAllUsers = router.query.userIdxFromAllUsers
//         ? parseInt(router.query.userIdxFromAllUsers, DECIMAL_RADIX) : 0;
//     const routerHasMorePages = router.query.hasMorePages === "true";
//     const routerSearchPrefix = router.query.searchPrefix ? router.query.searchPrefix : "";
//
//     const [filteredUsers, setFilteredUsers] = useState([]);
//     const [userIdxFromAllUsers, setUserIdxFromAllUsers] = useState(routerUserIdxFromAllUsers);
//     const [hasMorePages, setHasMorePages] = useState(routerHasMorePages);
//     const [searchPrefix, setSearchPrefix] = useState(routerSearchPrefix);
//
//     // const [searchPrefix, setSearchPrefix] = useDebouncedState(
//     //     routerSearchPrefix,
//     //     (search) => {
//     //         //setPrevSearchPrefix(searchPrefix) //todo - will it know theses 2 from outside?
//     //         console.log("trying to push to router in UsersContainer in config of useDebouncedState. hasMorePages:", hasMorePages)
//     //         setTriggerRouteUpdate(true);
//     //         console.log("hasMorePages in useDebouncedState:", hasMorePages)
//     //         return router.push({ pathname: '/auth/users', query: { ...router.query, searchPrefix: search, userIdxFromAllUsers: 0, hasMorePages } });
//     //     }
//     // );
//
//     const [searchInput, setSearchInput] = useDebouncedState(
//         routerSearchPrefix,
//         () => {
//             setSearchPrefix(routerSearchPrefix);
//             setUserIdxFromAllUsers(0);
//             return;
//         }
//     );
//
//     useEffect(() => { setSelected([]); }, [refresh]);
//
//     const authCapabilities = useAPI(() => auth.getAuthCapabilities());
//
//     useEffect(() => {
//         console.log("in useEffect that filters. searchPrefix:", searchPrefix)
//         if (allUsers) {
//             // if (prevAllUsersLen === 0) {
//             //     setPrevAllUsersLen(allUsers.length)
//             // }
//
//             let filtered;
//             // const toFilterByPrevFilteredUsers = prevAllUsersLen === allUsers.length && filteredUsers.length > 0
//             //     && searchPrefix.startsWith(prevSearchPrefix) && searchPrefix.length > prevSearchPrefix.length;
//             const toFilterByPrevFilteredUsers = false;
//
//             if (toFilterByPrevFilteredUsers) {
//                 filtered = filteredUsers.filter((user) =>
//                     resolveUserDisplayName(user).startsWith(searchPrefix)
//                 );
//             } else {
//                 filtered = allUsers.filter((user) =>
//                     resolveUserDisplayName(user).startsWith(searchPrefix)
//                 );
//             }
//
//             setFilteredUsers(filtered);
//             setHasMorePages(((routerUserIdxFromAllUsers + DEFAULT_LISTING_AMOUNT) < filtered.length));
//             //setPrevAllUsersLen(allUsers.length)
//         }
//     }, [allUsers, searchPrefix]);
//
//
//     const paginatedResults = useMemo(() => {
//         console.log("in paginatedResults. routerUserIdxFromAllUsers:", routerUserIdxFromAllUsers)
//         console.log("filteredUsers", filteredUsers)
//         setHasMorePages(((routerUserIdxFromAllUsers + DEFAULT_LISTING_AMOUNT) < filteredUsers.length));
//         return filteredUsers.slice(routerUserIdxFromAllUsers, routerUserIdxFromAllUsers + DEFAULT_LISTING_AMOUNT);
//     }, [filteredUsers, routerUserIdxFromAllUsers]);
//
//     // useEffect(() => {
//     //     console.log("in useEffect that pushes to router. ")
//     //     console.log("triggerRouteUpdate", triggerRouteUpdate)
//     //     console.log("hasMorePages in useEffect", hasMorePages)
//     //     router.push({
//     //         pathname: "/auth/users",
//     //         //query: { ...router.query, hasMorePages }
//     //         query: { ...router.query, searchPrefix: routerSearchPrefix, userIdxFromAllUsers: routerUserIdxFromAllUsers, hasMorePages }
//     //     });
//     // }, [hasMorePages, triggerRouteUpdate]);
//
//     // useEffect(() => {
//     //     console.log("Unified route update effect");
//     //     router.push({
//     //         pathname: "/auth/users",
//     //         query: {
//     //             ...router.query,
//     //             searchPrefix,
//     //             userIdxFromAllUsers,
//     //             hasMorePages,
//     //         },
//     //     });
//     // }, [searchPrefix, userIdxFromAllUsers, hasMorePages]);
//
//     useEffect(() => {
//         console.log("Unified route update effect");
//         router.push({
//             pathname: "/auth/users",
//             query: {
//                 ...router.query,
//                 searchPrefix: searchInput,
//                 userIdxFromAllUsers,
//                 hasMorePages,
//             },
//         });
//     }, [searchInput, userIdxFromAllUsers, hasMorePages]);
//
//
//
//
//     const userIdxFromAllUsersFromRouterForPagination = router.query.userIdxFromAllUsers
//         ? parseInt(router.query.userIdxFromAllUsers, DECIMAL_RADIX) : 0;
//     const hasMorePagesFromRouterForPagination = router.query.hasMorePages === "true";
//
//     const after = userIdxFromAllUsersFromRouterForPagination === 0
//         ? "" : String(userIdxFromAllUsersFromRouterForPagination);
//     const nextPage = hasMorePagesFromRouterForPagination
//         ? String(userIdxFromAllUsersFromRouterForPagination + DEFAULT_LISTING_AMOUNT) : null;
//
//     if (error) return <AlertError error={error}/>;
//     if (loading) return <Loading/>;
//     if (authCapabilities.loading) return <Loading/>;
//
//     const canInviteUsers = !authCapabilities.error && authCapabilities.response && authCapabilities.response.invite_user;
//
//     return (
//         <>
//             <ActionsBar>
//                 <UserActionsActionGroup canInviteUsers={canInviteUsers} selected={selected}
//                                         onClickInvite={() => setShowInvite(true)} onClickCreate={() => setShowCreate(true)}
//                                         onConfirmDelete={() => {
//                                             auth.deleteUsers(selected.map(u => u.id))
//                                                 .catch(err => setDeleteError(err))
//                                                 .then(() => {
//                                                     setSelected([]);
//                                                     setRefresh(!refresh);
//                                                 })}}/>
//                 <ActionGroup orientation="right">
//                     <InputGroup>
//                         <Form.Control
//                             autoFocus
//                             placeholder="Find a User..."
//                             value={searchInput}
//                             onChange={e => setSearchInput(e.target.value)}
//                         />
//                         <InputGroup.Text>
//                             <SearchIcon/>
//                         </InputGroup.Text>
//                     </InputGroup>
//                     <RefreshButton onClick={() => setRefresh(!refresh)}/>
//                 </ActionGroup>
//             </ActionsBar>
//             <div className="auth-learn-more">
//                 Users are entities that access and use lakeFS. <a href="https://docs.lakefs.io/reference/authentication.html" target="_blank" rel="noopener noreferrer">Learn more.</a>
//             </div>
//
//             {(!!deleteError) && <AlertError error={deleteError}/>}
//
//             <EntityActionModal
//                 show={showCreate}
//                 onHide={() => setShowCreate(false)}
//                 onAction={userId => {
//                     return auth.createUser(userId).then(() => {
//                         setSelected([]);
//                         setShowCreate(false);
//                         setRefresh(!refresh);
//                     });
//                 }}
//                 title={canInviteUsers ? "Create API User" : "Create User"}
//                 placeholder={canInviteUsers ? "Name (e.g. Spark)" : "Username (e.g. 'jane.doe')"}
//                 actionName={"Create"}
//                 validationFunction={disallowPercentSign(INVALID_USER_NAME_ERROR_MESSAGE)}
//             />
//
//             <EntityActionModal
//                 show={showInvite}
//                 onHide={() => setShowInvite(false)}
//                 onAction={async (userEmail) => {
//                     if (!validator.isEmail(userEmail)) {
//                         throw new Error("Invalid email address");
//                     }
//                     await auth.createUser(userEmail, true);
//                     setSelected([]);
//                     setShowInvite(false);
//                     setRefresh(!refresh);
//                 }}
//                 title={"Invite User"}
//                 placeholder={"Email"}
//                 actionName={"Invite"}
//             />
//
//             <DataTable
//                 results={paginatedResults}
//                 headers={['', 'User ID', 'Created At']}
//                 keyFn={user => user.id}
//                 rowFn={user => [
//                     <Checkbox
//                         disabled={(!!currentUser && currentUser.id === user.id)}
//                         name={user.id}
//                         onAdd={() => setSelected([...selected, user])}
//                         onRemove={() => setSelected(selected.filter(u => u !== user))}
//                     />,
//                     <Link href={{pathname: '/auth/users/:userId', params: {userId: user.id}}}>
//                         { resolveUserDisplayName(user) }
//                     </Link>,
//                     <FormattedDate dateValue={user.creation_date}/>
//                 ]}
//                 firstFixedCol={true}
//             />
//
//             <Paginator
//                 nextPage={nextPage}
//                 after={after}
//                 onPaginate={newOffset => setUserIdxFromAllUsers(newOffset ? parseInt(newOffset, DECIMAL_RADIX) : 0)}
//                 // onPaginate={(newOffset) => router.push({
//                 //     pathname: "/auth/users",
//                 //     query: {
//                 //         ...router.query,
//                 //         searchPrefix,
//                 //         userIdxFromAllUsers: newOffset ? parseInt(newOffset, DECIMAL_RADIX) : 0,
//                 //         hasMorePages }
//                 // })}
//             />
//         </>
//     );
// };
//
// const UserActionsActionGroup = ({canInviteUsers, selected, onClickInvite, onClickCreate, onConfirmDelete }) => {
//
//     return (
//         <ActionGroup orientation="left">
//             <Button
//                 hidden={!canInviteUsers}
//                 variant="primary"
//                 onClick={onClickInvite}>
//                 Invite User
//             </Button>
//
//             <Button
//                 variant="success"
//                 onClick={onClickCreate}>
//                 {canInviteUsers ? "Create API User" : "Create User"}
//             </Button>
//             <ConfirmationButton
//                 onConfirm={onConfirmDelete}
//                 disabled={(selected.length === 0)}
//                 variant="danger"
//                 msg={`Are you sure you'd like to delete ${selected.length} users?`}>
//                 Delete Selected
//             </ConfirmationButton>
//         </ActionGroup>
//     );
// }
//
// export const UsersPage = () => {
//     const { setActiveTab, refresh, setRefresh, allUsers, loading, error } = useOutletContext();
//     useEffect(() => setActiveTab("users"), [setActiveTab]);
//     return (
//         <UsersContainer
//             refresh={refresh}
//             setRefresh={setRefresh}
//             allUsers={allUsers}
//             loading={loading}
//             error={error}
//         />
//     );
// };
//
// const UsersIndexPage = () => {
//     const [setActiveTab] = useOutletContext();
//     const [refresh, setRefresh] = useState(false);
//
//     const { response: allUsers, loading, error } = useAPI(() => {
//         return allUsersFromLakeFS(resolveUserDisplayName);
//     }, [refresh]);
//
//     const getUserEmailById = useCallback((id) => {
//         const userRecord = allUsers?.find(user => user.id === id);
//         // return something, so we don't completely break the state
//         // this can help us track down issues later on
//         if (!userRecord) {
//             return USER_NOT_FOUND;
//         }
//
//         return userRecord.email || userRecord.id;
//     }, [allUsers]);
//
//     return (
//         <GetUserEmailByIdContext.Provider value={getUserEmailById}>
//             <Outlet context={{ setActiveTab, refresh, setRefresh, allUsers, loading, error, getUserEmailById }} />
//         </GetUserEmailByIdContext.Provider>
//     )
// }
//
// export default UsersIndexPage;


import React, {createContext, useCallback, useEffect, useMemo, useState} from "react";
import {Outlet, useOutletContext} from "react-router-dom";

import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import InputGroup from "react-bootstrap/InputGroup";
import {SearchIcon} from "@primer/octicons-react";

import {useAPI} from "../../../lib/hooks/api";
import {auth} from "../../../lib/api";
import useUser from "../../../lib/hooks/user";
import {ConfirmationButton} from "../../../lib/components/modals";
import {EntityActionModal} from "../../../lib/components/auth/forms";
import {Paginator} from "../../../lib/components/pagination";
import {Link} from "../../../lib/components/nav";
import {
    ActionGroup,
    ActionsBar,
    AlertError,
    Checkbox,
    DataTable,
    FormattedDate,
    Loading,
    RefreshButton,
    useDebouncedState
} from "../../../lib/components/controls";
import validator from "validator/es";
import {disallowPercentSign, INVALID_USER_NAME_ERROR_MESSAGE} from "../validation";
import {resolveUserDisplayName} from "../../../lib/utils";
import {allUsersFromLakeFS} from "../../../lib/components/auth/users";
import {useRouter} from "../../../lib/hooks/router";

const DEFAULT_LISTING_AMOUNT = 10;
const DECIMAL_RADIX = 10;
const USER_NOT_FOUND = "unknown";
export const GetUserEmailByIdContext = createContext();


const UsersContainer = ({ refresh, setRefresh, allUsers, loading, error }) => {
    const { user } = useUser();
    const currentUser = user;

    const router = useRouter();
    const [selected, setSelected] = useState([]);
    const [deleteError, setDeleteError] = useState(null);
    const [showCreate, setShowCreate] = useState(false);
    const [showInvite, setShowInvite] = useState(false);

    const routerUserIdxFromAllUsers = router.query.userIdxFromAllUsers
        ? parseInt(router.query.userIdxFromAllUsers, DECIMAL_RADIX) : 0;
    const routerHasMorePages = router.query.hasMorePages === "true";
    const routerSearchPrefix = router.query.searchPrefix ? router.query.searchPrefix : "";

    const [filteredUsers, setFilteredUsers] = useState([]);
    const [userIdxFromAllUsers, setUserIdxFromAllUsers] = useState(routerUserIdxFromAllUsers);
    const [hasMorePages, setHasMorePages] = useState(routerHasMorePages);

    // it doesn't work - reproduce: refresh -> go to Users -> type "ab" -> now there are no pagination buttons.
    // The reason is that it overrides the "hasMorePages" with the old state it detected. setTriggerRouteUpdate(true);
    // didn't help also to go to the route useEffect (the trigger didn't cause the useEffect to work).
    // I used here the states: "userIdxFromAllUsers" and "hasMorePages" because i wanted them to be updated correcly.
    // i tried to use just the "hasMorePages" but then I had to update it's new state on route and updating routes in different places causes
    // routes overrides. so i wanted to update in the last useEffect it's most accurate state but the the function in
    // useDebouncedState overrode it.
    // In addition this option causes the filter function to work on each letter typed because i intoduced only "searchPrefix"
    // without the "searchInput".
    const [searchPrefix, setSearchPrefix] = useDebouncedState(
        routerSearchPrefix,
        (search) => {
            console.log("trying to push to router in UsersContainer in config of useDebouncedState. hasMorePages:", hasMorePages)
            //setTriggerRouteUpdate(true);
            console.log("hasMorePages in useDebouncedState:", hasMorePages)
            return router.push({ pathname: '/auth/users', query: { ...router.query, searchPrefix: search, userIdxFromAllUsers: 0, hasMorePages } });
        }
    );

    useEffect(() => { setSelected([]); }, [refresh]);

    const authCapabilities = useAPI(() => auth.getAuthCapabilities());

    useEffect(() => {
        console.log("in useEffect that filters. searchPrefix:", searchPrefix)
        if (allUsers) {

            let filtered = allUsers.filter((user) =>
                resolveUserDisplayName(user).startsWith(searchPrefix)
            );

            setFilteredUsers(filtered);
            setHasMorePages(((userIdxFromAllUsers + DEFAULT_LISTING_AMOUNT) < filtered.length));
        }
    }, [allUsers, searchPrefix]);


    const paginatedResults = useMemo(() => {
        console.log("in paginatedResults. routerUserIdxFromAllUsers:", routerUserIdxFromAllUsers)
        console.log("filteredUsers", filteredUsers)
        setHasMorePages(((userIdxFromAllUsers + DEFAULT_LISTING_AMOUNT) < filteredUsers.length));
        return filteredUsers.slice(userIdxFromAllUsers, userIdxFromAllUsers + DEFAULT_LISTING_AMOUNT);
    }, [filteredUsers, userIdxFromAllUsers]);

    // useEffect(() => {
    //     console.log("in useEffect that pushes to router. ")
    //     console.log("triggerRouteUpdate", triggerRouteUpdate)
    //     console.log("hasMorePages in useEffect", hasMorePages)
    //     router.push({
    //         pathname: "/auth/users",
    //         //query: { ...router.query, hasMorePages }
    //         query: { ...router.query, searchPrefix: routerSearchPrefix, userIdxFromAllUsers: routerUserIdxFromAllUsers, hasMorePages }
    //     });
    // }, [hasMorePages, triggerRouteUpdate]);

    useEffect(() => {
        console.log(" route update effect");
        router.push({
            pathname: "/auth/users",
            query: {
                ...router.query,
                searchPrefix,
                userIdxFromAllUsers,
                hasMorePages,
            },
        });
    }, [searchPrefix, userIdxFromAllUsers, hasMorePages]);


    const userIdxFromAllUsersFromRouterForPagination = router.query.userIdxFromAllUsers
        ? parseInt(router.query.userIdxFromAllUsers, DECIMAL_RADIX) : 0;
    const hasMorePagesFromRouterForPagination = router.query.hasMorePages === "true";

    const after = userIdxFromAllUsersFromRouterForPagination === 0
        ? "" : String(userIdxFromAllUsersFromRouterForPagination);
    const nextPage = hasMorePagesFromRouterForPagination
        ? String(userIdxFromAllUsersFromRouterForPagination + DEFAULT_LISTING_AMOUNT) : null;

    if (error) return <AlertError error={error}/>;
    if (loading) return <Loading/>;
    if (authCapabilities.loading) return <Loading/>;

    const canInviteUsers = !authCapabilities.error && authCapabilities.response && authCapabilities.response.invite_user;

    return (
        <>
            <ActionsBar>
                <UserActionsActionGroup canInviteUsers={canInviteUsers} selected={selected}
                                        onClickInvite={() => setShowInvite(true)} onClickCreate={() => setShowCreate(true)}
                                        onConfirmDelete={() => {
                                            auth.deleteUsers(selected.map(u => u.id))
                                                .catch(err => setDeleteError(err))
                                                .then(() => {
                                                    setSelected([]);
                                                    setRefresh(!refresh);
                                                })}}/>
                <ActionGroup orientation="right">
                    <InputGroup>
                        <Form.Control
                            autoFocus
                            placeholder="Find a User..."
                            value={searchPrefix}
                            onChange={e => setSearchPrefix(e.target.value)}
                        />
                        <InputGroup.Text>
                            <SearchIcon/>
                        </InputGroup.Text>
                    </InputGroup>
                    <RefreshButton onClick={() => setRefresh(!refresh)}/>
                </ActionGroup>
            </ActionsBar>
            <div className="auth-learn-more">
                Users are entities that access and use lakeFS. <a href="https://docs.lakefs.io/reference/authentication.html" target="_blank" rel="noopener noreferrer">Learn more.</a>
            </div>

            {(!!deleteError) && <AlertError error={deleteError}/>}

            <EntityActionModal
                show={showCreate}
                onHide={() => setShowCreate(false)}
                onAction={userId => {
                    return auth.createUser(userId).then(() => {
                        setSelected([]);
                        setShowCreate(false);
                        setRefresh(!refresh);
                    });
                }}
                title={canInviteUsers ? "Create API User" : "Create User"}
                placeholder={canInviteUsers ? "Name (e.g. Spark)" : "Username (e.g. 'jane.doe')"}
                actionName={"Create"}
                validationFunction={disallowPercentSign(INVALID_USER_NAME_ERROR_MESSAGE)}
            />

            <EntityActionModal
                show={showInvite}
                onHide={() => setShowInvite(false)}
                onAction={async (userEmail) => {
                    if (!validator.isEmail(userEmail)) {
                        throw new Error("Invalid email address");
                    }
                    await auth.createUser(userEmail, true);
                    setSelected([]);
                    setShowInvite(false);
                    setRefresh(!refresh);
                }}
                title={"Invite User"}
                placeholder={"Email"}
                actionName={"Invite"}
            />

            <DataTable
                results={paginatedResults}
                headers={['', 'User ID', 'Created At']}
                keyFn={user => user.id}
                rowFn={user => [
                    <Checkbox
                        disabled={(!!currentUser && currentUser.id === user.id)}
                        name={user.id}
                        onAdd={() => setSelected([...selected, user])}
                        onRemove={() => setSelected(selected.filter(u => u !== user))}
                    />,
                    <Link href={{pathname: '/auth/users/:userId', params: {userId: user.id}}}>
                        { resolveUserDisplayName(user) }
                    </Link>,
                    <FormattedDate dateValue={user.creation_date}/>
                ]}
                firstFixedCol={true}
            />

            <Paginator
                nextPage={nextPage}
                after={after}
                onPaginate={newOffset => setUserIdxFromAllUsers(newOffset ? parseInt(newOffset, DECIMAL_RADIX) : 0)}
                // onPaginate={(newOffset) => router.push({
                //     pathname: "/auth/users",
                //     query: {
                //         ...router.query,
                //         searchPrefix,
                //         userIdxFromAllUsers: newOffset ? parseInt(newOffset, DECIMAL_RADIX) : 0,
                //         hasMorePages }
                // })}
            />
        </>
    );
};

const UserActionsActionGroup = ({canInviteUsers, selected, onClickInvite, onClickCreate, onConfirmDelete }) => {

    return (
        <ActionGroup orientation="left">
            <Button
                hidden={!canInviteUsers}
                variant="primary"
                onClick={onClickInvite}>
                Invite User
            </Button>

            <Button
                variant="success"
                onClick={onClickCreate}>
                {canInviteUsers ? "Create API User" : "Create User"}
            </Button>
            <ConfirmationButton
                onConfirm={onConfirmDelete}
                disabled={(selected.length === 0)}
                variant="danger"
                msg={`Are you sure you'd like to delete ${selected.length} users?`}>
                Delete Selected
            </ConfirmationButton>
        </ActionGroup>
    );
}

export const UsersPage = () => {
    const { setActiveTab, refresh, setRefresh, allUsers, loading, error } = useOutletContext();
    useEffect(() => setActiveTab("users"), [setActiveTab]);
    return (
        <UsersContainer
            refresh={refresh}
            setRefresh={setRefresh}
            allUsers={allUsers}
            loading={loading}
            error={error}
        />
    );
};

const UsersIndexPage = () => {
    const [setActiveTab] = useOutletContext();
    const [refresh, setRefresh] = useState(false);

    const { response: allUsers, loading, error } = useAPI(() => {
        return allUsersFromLakeFS(resolveUserDisplayName);
    }, [refresh]);

    const getUserEmailById = useCallback((id) => {
        const userRecord = allUsers?.find(user => user.id === id);
        // return something, so we don't completely break the state
        // this can help us track down issues later on
        if (!userRecord) {
            return USER_NOT_FOUND;
        }

        return userRecord.email || userRecord.id;
    }, [allUsers]);

    return (
        <GetUserEmailByIdContext.Provider value={getUserEmailById}>
            <Outlet context={{ setActiveTab, refresh, setRefresh, allUsers, loading, error, getUserEmailById }} />
        </GetUserEmailByIdContext.Provider>
    )
}

export default UsersIndexPage;
