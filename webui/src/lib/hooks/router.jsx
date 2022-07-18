import { useHistory, useLocation, useParams, generatePath } from "react-router-dom"

export const useQuery = () => {
    const location = useLocation();
    const result = {};
    for(const [key, value] of (new URLSearchParams(location.search)).entries()) {
        result[key] = value;
    }
    return result;
}

export const buildURL = (url) => {
    if (typeof url === 'string') return url;
    // otherwise, assume query, params and pathname
    const path = generatePath(url.pathname, (url.params) ? url.params : {})
    if (!url.query) return path;
    const query = new URLSearchParams(url.query).toString();
    return `${path}?${query}`;
}

export const useRouter = () => {
    const location = useLocation();
    const history = useHistory();
    const query = useQuery();
    const params = useParams();
    return {
        query,
        params,
        route: location.pathname,
        history: history,
        push: (url) => history.push(buildURL(url))
    };
};