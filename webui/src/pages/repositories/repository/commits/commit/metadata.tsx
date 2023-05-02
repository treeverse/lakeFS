import React from "react";
import Button from 'react-bootstrap/Button';

const keyIsClickableUrl = /::lakefs::(.*)::url\[url:ui\]$/;

export const MetadataRow = ({ metadata_key, metadata_value }) => {
    return <tr>
               <td><code>{metadata_key}</code></td>
               <td><code>{metadata_value}</code></td>
           </tr>;
};

export const MetadataUIButton = ({ metadata_key, metadata_value }) => {
    const m = metadata_key.match(keyIsClickableUrl);
    if (!m) {
        return null;
    }
    return <tr key={metadata_key}>
               <td colSpan={2}>
                   <Button variant="success" href={metadata_value}>Open {m[1]} UI</Button>
               </td>
           </tr>;
};
