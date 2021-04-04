import Button from "react-bootstrap/Button";
import React from "react";


export const Paginator = ({ onPaginate, nextPage = null, after = "" }) => {
    return (
        <div className="paginator mt-2 mb-2">

            <Button variant="secondary" disabled={!after} onClick={() => onPaginate("")}>Back to Start</Button>
            {' '}

            <Button variant="primary" disabled={(nextPage === null)} onClick={() => onPaginate(nextPage)}>Next Page</Button>

        </div>
    )
}