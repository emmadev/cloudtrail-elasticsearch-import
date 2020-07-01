import * as AWS from "aws-sdk";
import {Readable} from "stream";
import streamifier from "streamifier";

// @ts-ignore
export class S3Fake implements AWS.S3 {
    private readonly buckets: {[bucket: string]: {[key: string]: Uint8Array}} = {};

    private async serialize(body: Buffer | Uint8Array | string | Readable): Promise<Uint8Array> {
        if (body instanceof Uint8Array) {
            return body;
        } else if (body instanceof Readable) {
            return await new Promise<Uint8Array>((resolve, reject) => {
                const bufs: Uint8Array[] = [];
                body.on('data', data => bufs.push(data));
                body.on('end', () => resolve(Buffer.concat(bufs)));
                body.on('error', err => reject(err));
            });
        } else {
            return Buffer.from(body, 'utf8');
        }
    }

    // @ts-ignore
    createBucket(params: {
        Bucket: string
    }) {
        return {
            promise: async () => {
                this.buckets[params.Bucket] = this.buckets[params.Bucket] || {};
                return {};
            }
        }
    }

    // @ts-ignore
    putObject(params: {
        Body: Buffer | Uint8Array | string | Readable,
        Bucket: string,
        Key: string,
    }) {
        return {
            promise: (async () => {
                this.buckets[params.Bucket] = this.buckets[params.Bucket] || {};
                this.buckets[params.Bucket][params.Key] = await this.serialize(params.Body);
                return {};
            })
        };
    }

    // @ts-ignore
    listObjectsV2(params: {Bucket: string, Prefix?: string | undefined, MaxKeys?: number | undefined, ContinuationToken?: string | undefined}) {
        const bucket = this.buckets[params.Bucket] || {};

        const keys = Object.keys(bucket);
        const prefix = params.Prefix;
        const maxKeys = params.MaxKeys || 1000;
        const marker = params.ContinuationToken;
        const startIndex = marker ? parseInt(marker) : 0;
        const endIndex = startIndex + maxKeys;

        let objects = prefix ? keys.filter(key => key.startsWith(prefix)) : keys;
        const nextMarker = endIndex >= objects.length ? null : endIndex.toString();
        objects = objects.slice(startIndex, endIndex);

        return {
            promise: (async () => ({
                Contents:
                    objects.map(key => ({
                        Key: key,
                    }))
                ,
                IsTruncated: !!nextMarker,
                NextContinuationToken: nextMarker,
            }))
        };
    }

    // @ts-ignore
    getObject(params: { Bucket: string, Key: string }) {
        const bucket = this.buckets[params.Bucket] || {};
        const contents = bucket[params.Key];
        return {
            createReadStream: () => streamifier.createReadStream(contents)
        };
    }

    // @ts-ignore
    deleteBucket(params: { Bucket: string }) {
        if(this.buckets[params.Bucket]) {
            delete this.buckets[params.Bucket];
        }
        return {
            promise: async () => ({})
        };
    }
}

export const createS3Fake = (): AWS.S3 => (new S3Fake() as unknown as AWS.S3);
