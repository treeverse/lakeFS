import useSWR from 'swr'
import  { useEffect } from 'react'
import {Router} from 'next/router'

async function getUser() {
    // check localstorage for the existence of a user
    const userData = window.localStorage.getItem("user")
    if (userData !== null) {
        return {
            details: JSON.parse(userData),
            loggedIn: true
        }
    }
    return {
        loggedIn: false
    }
}

function useUser({redirectTo = false} = {}) {
    const { data: user, mutate: mutateUser } = useSWR("auth/user", getUser);
    useEffect(() => {
        // if no redirect needed, just return (example: already on /dashboard)
        // if user data not yet there (fetch in progress, logged in or not) then don't do anything yet
        if (!redirectTo || !user) return;
        if (redirectTo && user && user.loggedIn)  Router.push(redirectTo);
    }, [user, redirectTo]);
    return { user, mutateUser };
}

export default useUser