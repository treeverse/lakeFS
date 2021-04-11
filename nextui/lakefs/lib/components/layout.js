import Container from 'react-bootstrap/Container'
import TopNav from './navbar';

const Layout = ({ children, fluid = "xl" }) => {
    return (
        <>
            <TopNav/>

            <div className="main-app">
                {children}
            </div>
        </>
    )
}

export default Layout;