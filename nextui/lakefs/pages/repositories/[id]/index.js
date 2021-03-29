import Layout from '../../../lib/components/layout';
import {useRouter} from "next/router";
import {useEffect} from "react";

const RepositoryIndexPage = () => {
    const router = useRouter()
    const { id } = router.query;
    useEffect(() => {
        if (!!id) router.push(`/repositories/${encodeURIComponent(id)}/objects`)
    }, [])
    return (
        <Layout/>
    )
}

export default RepositoryIndexPage;