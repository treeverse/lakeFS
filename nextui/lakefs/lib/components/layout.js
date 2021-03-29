import Container from 'react-bootstrap/Container'
import TopNav from './navbar';

const Layout = ({ children }) => {
    return (
        <>
            <TopNav/>
            <Container className="main-app">
                {children}
            </Container>
        </>
    )
}

export default Layout;