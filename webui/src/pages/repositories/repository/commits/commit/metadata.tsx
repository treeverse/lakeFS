import React, { useCallback } from "react";
import Button from 'react-bootstrap/Button';
import { statistics } from "../../../../../lib/api";

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
    const matchType = m[1];
    const click = useCallback(() => gotoMetadata(matchType, metadata_value), [matchType, metadata_value]);
    return <tr key={metadata_key}>
        <td colSpan={2}>
            <Button variant="success" onClick={click}>Open {matchType} UI</Button>
        </td>
    </tr>;
};
