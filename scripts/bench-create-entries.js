import http from "k6/http";
import encoding from 'k6/encoding';

const access_key_id = 'AKIAJVD5P3WTAFH7IN5Q';
const access_secret_key = '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';

const credentials = `${access_key_id}:${access_secret_key}`;
const encodedCredentials = encoding.b64encode(credentials);

function uuidv4() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

export default function() {
    const reqId = uuidv4();
    const file = reqId;

    const payload = {
        content: http.file(`content_${reqId}`, "file.txt"),
    };

    const params = {
        headers: {
            'Authorization': `Basic ${encodedCredentials}`,
        },
    };

    const res = http.post(http.url`http://127.0.0.1:8001/api/v1/repositories/repo1/branches/master/objects?loader-request-type=createFile&path=file_${reqId}`,
        payload, params);
    //console.log(JSON.stringify(res, null, 2))
}
