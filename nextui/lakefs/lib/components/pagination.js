import Button from "react-bootstrap/Button";
import React from "react";


export const Paginator = ({ hasMore, paginate  }) => {
    return (hasMore) ? (
        <div className="paginator mt-2 mb-2">
            <Button variant="outline-primary" onClick={paginate}>Load More</Button>
        </div>
    ) : <></>
}