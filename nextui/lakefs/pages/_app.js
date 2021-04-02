import 'bootswatch/dist/lumen/bootstrap.css'
import '../styles/globals.css'

export function reportWebVitals(metric) {
  console.log(metric)
}

function MyApp({ Component, pageProps }) {
  return (
      <Component {...pageProps} />
  )
}

export default MyApp
