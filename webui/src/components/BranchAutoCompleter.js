import {AsyncTypeahead} from "react-bootstrap-typeahead";
import React from "react";


export default ({ id, branchList, searchBranch, onChange }) => {
    return (
        <AsyncTypeahead
            id={id}
            isLoading={false}
            options={branchList}
            onSearch={(query) => { searchBranch(query); }}
            onChange={onChange}
            placeholder="Search for a branch..."/>
    );
};

