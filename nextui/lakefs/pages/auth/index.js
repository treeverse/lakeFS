import {useRouter} from "next/router";
import {useEffect} from "react";
import Layout from "../../lib/components/layout";

const AuthHome = () => {
    const router = useRouter()
    useEffect(() => {
        router.push('/auth/credentials')
    }, [])
    return (
        <Layout/>
    )
}

export default AuthHome
