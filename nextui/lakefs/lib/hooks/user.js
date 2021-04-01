import {useEffect, useState} from 'react'
import {Router} from 'next/router'

function useCachedUser() {
    // check localstorage for the existence of a user
    const [user, setUser] =  useState({loggedIn: false})

    useEffect(() => {
        const userData = localStorage.getItem("user")
        if (!userData) return
         setUser({
            details: JSON.parse(userData),
            loggedIn: true
        })
    }, [user.loggedIn, setUser])

    return user
}

function useUser({redirectTo = false} = {}) {

    const user = useCachedUser()

    useEffect(() => {
        // if no redirect needed, just return (example: already on /dashboard)
        // if user data not yet there (fetch in progress, logged in or not) then don't do anything yet
        if (!redirectTo || !user) return;
        if (redirectTo && user && user.loggedIn)  Router.push(redirectTo);
    }, [redirectTo]);

    return { user };
}

export default useUser