import {useRouter} from "next/router";
import {useEffect} from "react";
import Layout from "../../../../lib/components/layout";


export default function Home() {
    const router = useRouter()
    const { groupId } = router.query
    useEffect(() => {
        if (!!groupId)  router.push({
            pathname: '/auth/groups/[groupId]/members',
            query: {groupId}
        })
    }, [groupId])

    return <Layout/>
}
