// styles

import Layout from '../lib/components/layout';
import {useRouter} from "next/router";
import {useEffect} from "react";

// main app
export default function Home() {
  const router = useRouter()
  useEffect(() => {
    router.push('/repositories')
  }, [])
  return (
      <Layout/>
  )
}
