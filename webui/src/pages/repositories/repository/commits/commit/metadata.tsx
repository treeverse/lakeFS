import React, {useCallback} from "react";
import Button from 'react-bootstrap/Button';
import {statistics} from "../../../../../lib/api";

const keyIsClickableUrl = /::lakefs::(.*)::url\[ur[il]:ui\]$/;

export const MetadataRow = ({ metadata_key, metadata_value }) => {
    return <tr>
               <td><code>{metadata_key}</code></td>
               <td><code>{metadata_value}</code></td>
           </tr>;
};

export const gotoMetadata = async (typ, url) => {
    const event = {
	class: "integration",
	name: 'link',
	type: typ,
	count: 1,
    };
    // Just ignore any errors ins statistics.
    await statistics.postStatsEvents([event]).catch(() => null);
    window.location = url;
}

export const MetadataUIButton = ({ metadata_key, metadata_value }) => {
    const m = metadata_key.match(keyIsClickableUrl);
    if (!m) {
        return null;
    }
    const click = useCallback(() => gotoMetadata(m[1], metadata_value), [m[1], metadata_value]);
    return <tr key={metadata_key}>
               <td colSpan={2}>
                   <Button variant="success" onClick={click}>Open {m[1]} UI</Button>
               </td>
           </tr>;
};
