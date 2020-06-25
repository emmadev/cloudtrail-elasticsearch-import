import {ListObjectsOutput, ListObjectsRequest, Object} from "aws-sdk/clients/s3";
import {CloudtrailLog, CloudtrailLogRecord} from "../types/log/cloudtrail-log";
import {Program} from "../types/input/program";
import * as AWS from "aws-sdk";
import debug from "debug";
import {ExtractionParams} from "./params";
import {Client as ESClient} from "@elastic/elasticsearch";
import * as zlib from "zlib";
import * as Either from "fp-ts/lib/Either";
import * as Tree from "fp-ts/lib/Tree";
import {DecodeError} from "io-ts/lib/Decoder";
import {getId} from "../common/get-id";
import {Readable} from "stream";
import {nestedYield} from "../common/nested-yield";
import moment from "moment";

const listS3Objs = async function* (s3: AWS.S3, program: Program): AsyncGenerator<Object, void> {
    const d = debug("listS3Objs");

    const params: ListObjectsRequest = {Bucket: program.bucket, Prefix: program.prefix};

    try {
        let objs: ListObjectsOutput;
        do {
            /* fetch a list of all the objects to be processed based on the prefix */
            d("Fetching %s with marker: %s", program.prefix, params.Marker);
            objs = await s3.listObjects(params).promise();
            if (objs.Contents) {
                for (const obj of objs.Contents) {
                    yield obj;
                }
                if (objs.IsTruncated) {
                    params.Marker = objs.NextMarker;
                }
            }
        } while (objs.IsTruncated);
    } catch (e) {
        d("ERROR: %s", e);
        throw e;
    }
};

export const alreadyFinished = async (program: Program, es: ESClient, key: string): Promise<boolean> => {
    const d = debug("alreadyFinished");
    const id = getId(program.bucket, key);
    const res = await es.get({ id, index: program.workIndex }, { ignore: [404] });
    if (res.statusCode === 200) {
        return true;
    }
    if (res.statusCode !== 404) {
        d(`Error looking up ${id}: ${res.statusCode} ${res.body}`);
        return false;
    }
    return false;
};

export const markFinished = async (program: Program, es: ESClient, key: string): Promise<void> => {
    await es.index({
        index: program.workIndex,
        id: getId(program.bucket, key),
        body: {
            key: key,
            timestamp: moment().format(),
        }
    });
};

export const readGzipS3Stream = (s3: AWS.S3, bucket: string, key: string): Readable =>
    s3.getObject({
        Bucket: bucket,
        Key: key,
    })
    .createReadStream()
    .pipe(zlib.createGunzip());

export const readAll = async (readable: Readable): Promise<string> => {
    let body = "";
    for await(const data of readable) {
        body += data;
    }
    return body;
};

export const parseCloudtrailLog = (key: string, jsonSrc: string): CloudtrailLog | null => {
    const d = debug("parseCloudtrailLog");
    let json: unknown;
    try {
        json = Either.parseJSON(jsonSrc, e => Tree.make(String(e)));
    } catch (e) {
        d(`Error parsing S3 log at ${key}: ${e}`);
        return null;
    }
    const validation = CloudtrailLog.decode(json);
    return Either.getOrElseW((e: DecodeError) => {
        d(`S3 log invalid at ${key}: ${e}`);
        return null;
    })(validation);
};

/**
 * Ensures the necessary ElasticSearch Indexes Exist
 *
 * @method ensureIndexes
 * @param {ESClient} ES initialized Elastical.Client
 * @param {String} workIndexName name of index for keeping track of processed objects
 * @param {String} cloudtrailIndexName name of index for cloudtrail events
 * @return {Promise<void>}
 */
const ensureIndexes = async (ES: ESClient, workIndexName: string, cloudtrailIndexName: string) => {
    const d = debug("ensureIndexes");
    const indices = ES.indices;
    const [workIndex, CTIndex] =
        await Promise.all([
            indices.exists({index: workIndexName}),
            indices.exists({index: cloudtrailIndexName}),
        ]);

    const makeWorkIndex = () =>
        ES.indices.create({
            index: workIndexName,
            body: {
                mappings: {
                    properties: {
                        eventTime: {type: "date"}
                    }
                }
            },
        });

    const makeCTIndex = () =>
        ES.indices.create({
            index: cloudtrailIndexName,
            body: {
                mappings: {
                    properties: {
                        eventTime: {type: "date", format: "date_time_no_millis"}
                    }
                }
            },
        });

    await Promise.all<unknown, unknown>([
        workIndex.body
            ? Promise.resolve(d(`${workIndexName} exists`))
            : makeWorkIndex(),
        CTIndex.body
            ? Promise.resolve(d(`${cloudtrailIndexName} exists`))
            : makeCTIndex(),
    ]);
};

export const cloudtrailLogRecordExtractor =
    ({program, s3, es}: ExtractionParams) =>
    (): AsyncGenerator<CloudtrailLogRecord, void> =>
        nestedYield((async function*(){
            const d = debug("cloudtrailLogRecordExtractor");
            await ensureIndexes(es, program.workIndex, program.cloudtrailIndex);
            for await (const obj of listS3Objs(s3, program)) {
                const key: string | undefined = obj.Key;
                if (!key) {
                    d(`Object ${obj} has no key; this should not happen.`);
                } else if (await alreadyFinished(program, es, key)) {
                    d(`Skip ${obj.Key}; already exists.`);
                } else {
                    yield((async function*() {
                        d(`Processing ${key}`);
                        const jsonSrc = await readAll(readGzipS3Stream(s3, program.bucket, key));
                        const log: CloudtrailLog | null = parseCloudtrailLog(key, jsonSrc);

                        if (log) {
                            for (const record of log.Records) {
                                yield { ...record };
                            }
                            await markFinished(program, es, key);
                        }
                    })());
                }
            }
        })());
