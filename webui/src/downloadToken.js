import moment from "moment";


const buf2hex = (buf) => {
    return Array.prototype.map.call(new Uint8Array(buf), x=>(('00'+x.toString(16)).slice(-2))).join('');
};

export const generateDownloadToken = async (key, secret, path) => {
    const enc = new TextEncoder('utf-8');

    const signingKey = await window.crypto.subtle.importKey(
        'raw',
        enc.encode(secret),
        {name:'HMAC',hash : 'SHA-256'},
        false, ['sign']
    );

    const currentTime = moment().unix();

    const token = await window.crypto.subtle.sign(
        {name: "HMAC", hash: "SHA-256"},
        signingKey,
        enc.encode(`${currentTime}/${key}/${path}`)
    );

     return `${buf2hex(token)}/${currentTime}/${key}/${path}`;
};