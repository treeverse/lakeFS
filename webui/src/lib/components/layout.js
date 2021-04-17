import TopNav from './navbar';

const Layout = ({ children }) => {
    return (
        <>
            <TopNav/>
            <div className="main-app">
                {children}
            </div>
        </>
    );
};

export default Layout;