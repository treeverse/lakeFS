import {useRouter} from "next/router";
import {useEffect} from "react";
import Layout from "../../../../lib/components/layout";


export default function Home() {
    const router = useRouter()
    const { userId } = router.query
    useEffect(() => {
        if (!!userId)  router.push({
            pathname: '/auth/users/[userId]/groups',
            query: {userId}
        })
    }, [userId])

    return <Layout/>
}
