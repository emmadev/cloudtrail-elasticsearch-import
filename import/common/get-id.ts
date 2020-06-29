import * as crypto from "crypto";

const createSigner = () => crypto.createHmac('sha256', 'cloudtrail-elasticsearch-import-C001D00D');

export const getId = (bucket: string, key: string): string =>
    createSigner()
        .update(key)
        .update(bucket)
        .digest()
        .toString("hex");
