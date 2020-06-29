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
import {Merge} from "../common/merge";
import moment from "moment";

/**
 * List all S3 objects at a given prefix
 *
 * @param s3 {AWS.S3} S3 Client
 * @param program {Program} Command line and environment variables
 * @yield {AWS.S3.Object} An S3 Object
 */
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

/**
 * Check if an S3 object has already been imported
 * @param program {Program} Command line and environment variables
 * @param es {ESClient} Elasticsearch client
 * @param key {string} S3 key to mark in Elasticsearch
 * @return {Promise<boolean>} Promise of whether the object is already imported.
 */
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

/**
 * Mark that we are finished importing an S3 object
 * @param program {Program} Command line and environment variables
 * @param es {ESClient} Elasticsearch client
 * @param key {string} S3 key to mark in Elasticsearch
 */
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

/**
 * Read a GZipped object from S3
 * @param s3 {AWS.S3} The S3 Client
 * @param bucket {string} The S3 Bucket
 * @param key {string} The key of the object in the bucket
 * @return A NodeJS stream of the unzipped S3 object.
 */
export const readGzipS3Stream = (s3: AWS.S3, bucket: string, key: string): Readable =>
    s3.getObject({
        Bucket: bucket,
        Key: key,
    })
    .createReadStream()
    .pipe(zlib.createGunzip());

/**
 * Asynchronously read a NodeJS stream
 * @param readable {Readable} A NodeJS stream
 * @return {Promise<string>} A promise of the complete stream in memory.
 */
export const readAll = async (readable: Readable): Promise<string> => {
    let body = "";
    for await(const data of readable) {
        body += data;
    }
    return body;
};

/**
 * Parse cloudtrail log entries from a raw JSON string.
 * @param key {string} The S3 Key (for error messages)
 * @param jsonSrc {string} The JSON to parse
 * @return {CloudtrailLog | null} The parsed log, or null (accompanied by an error
 *                                message in the batch log) if parse failed.
 */
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
 * @method ensureWorkIndex
 * @param {ESClient} ES initialized Elastical.Client
 * @param {String} workIndexName name of index for keeping track of processed objects
 * @return {Promise<void>}
 */
const ensureWorkIndex = async (ES: ESClient, workIndexName: string) => {
    const d = debug("ensureWorkIndex");
    const indices = ES.indices;
    const workIndex = await indices.exists({index: workIndexName});

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

    if (workIndex.body) {
        d(`${workIndexName} exists`);
    } else {
        await makeWorkIndex();
    }
};

/** Yields all records in an S3 object.
 *
 * @param s3 {AWS.S3}
 * @param es {ESClient}
 * @param program {Program} Command Line and Environment variables
 * @yield {CloudtrailLogRecord} The records in the S3 Object
 */
const eachRecord =
    (s3: AWS.S3, es: ESClient, program: Program) =>
    async function*(obj: AWS.S3.Object): AsyncIterable<CloudtrailLogRecord> {
        const d = debug("eachRecord");

        const key: string | undefined = obj.Key;
        if (!key) {
            d(`Object ${obj} has no key; this should never happen.`);
        } else if (await alreadyFinished(program, es, key)) {
            d(`Skip ${obj.Key}; already exists.`);
        } else {
            d(`Processing ${key}`);
            const jsonSrc = await readAll(readGzipS3Stream(s3, program.bucket, key));
            const log: CloudtrailLog | null = parseCloudtrailLog(key, jsonSrc);

            if (log) {
                for (const record of log.Records) {
                    yield {...record};
                }
                await markFinished(program, es, key);
            }
        }
    };

/**
 *
 * @param merge {Merge} An implementation of Merge on multiple AsyncIterators.
 * @param program {Program} Command Line and Environment variables
 * @param s3 {AWS.S3} S3 Client
 * @param es {ESClient} Elasticsearch Client
 */
export const cloudtrailLogRecordExtractor =
    (merge: Merge) =>
    ({program, s3, es}: ExtractionParams) =>
    async function*(): AsyncIterable<CloudtrailLogRecord> {
        await ensureWorkIndex(es, program.workIndex);
        return merge(eachRecord(s3, es, program))(listS3Objs(s3, program));
    };

