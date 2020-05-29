#!/usr/bin/env node

/*
 * Steps:
 *
 * - make sure indexes exist, create them if they don't
 * - do an S3 list of stuff under a specific prefix
 * - for each file, download it, unzip it and extract the json
 * - extract the events and put them into ElasticSearch
 * - record in elastic-search that the file was processed
 * - repeat
 */


const
    ES_FILE_CONCURRENCY = 1    /* how many concurrent S3 cloudtrail files to do */
;

import AWS from 'aws-sdk';
import zlib from 'zlib';
import { URL } from 'url';
import { Client as ESClient } from '@elastic/elasticsearch';
import moment from 'moment';
import debug from 'debug';
import * as crypto from 'crypto';
import * as t from 'io-ts';
import Either from "fp-ts/lib/Either";
import {ListObjectsOutput, ListObjectsRequest, Object} from "aws-sdk/clients/s3";
import { version } from './package.json';

const d = {
    ESError: debug("ElasticSearch:error"),
    info: debug("info"),
    error: debug("error")
};

const createSigner = () => crypto.createHmac('sha256','cloudtrail-elasticsearch-import-C001D00D');

const ProgramV = t.type({
    bucket: t.string,
    region: t.string,
    prefix: t.string,
    elasticsearch: t.string,
    workIndex: t.string,
    cloudtrailIndex: t.string
});

type Program = Readonly<t.TypeOf<typeof ProgramV>>;

const program: Program = (() => {
    const commander = require('commander');

    commander
        .version(version)
        .option('-b, --bucket <sourcebucket>', 'Bucket with cloudtrail logs', String, '')
        .option('-r, --region <bucket region>', 'Default region: us-east-1', String, 'us-east-1')
        .option('-p, --prefix <prefix>', 'prefix where to start listing objects')

        .option('-e, --elasticsearch <url>', 'ES base, ie: https://host:port', String, '')
        .option('--work-index <name>', 'ES index to record imported files, def: cloudtrail-imported', String, 'cloudtrail-import-log')
        .option('--cloudtrail-index <name>', 'ES index to put cloudtrail events, def: cloudtrail', String, 'cloudtrail')
        .parse(process.argv);

    return Either.fold(
        (err: t.Errors): Program => {
            d.error("FATAL - Unable to validate command line options: %s", err);
            process.exit(1)
        },
        (v: Program): Program => v,
    )(ProgramV.decode(commander));
})();

/*
 * TODO put some error / param checking code here
 */

const ES = (() => {
    if (program.elasticsearch) {
        const esUrl = new URL(program.elasticsearch);
        esUrl.port = esUrl.port || '9200';
        return new ESClient({
            node: {
                url: esUrl,
            },
        });
    } else {
        console.error("--elasticsearch required");
        process.exit(1);
    }
})();

/*
 * yah these need to exists
 */
AWS.config.update({
    accessKeyId: process.env.AWS_ACCESS_KEY,
    secretAccessKey: process.env.AWS_SECRET_KEY
});

const run = async () => {
    const [, s3Objs] = await Promise.all([
        ensureIndexes(ES, program.workIndex, program.cloudtrailIndex),
        generateS3Objs(),
    ]);
    await processItems(s3Objs);
};

run().catch(e => d.error("UNCAUGHT ERROR: %s", e));

/**
 * Ensures the necessary ElasticSearch Indexes Exist
 *
 * @method ensureIndexes
 * @param {ESClient} ES initialized Elastical.Client
 * @param {String} workIndexName name of index for keeping track of processed objects
 * @param {String} cloudtrailIndexName name of index for cloudtrail events
 * @return {Promise<void>}
 */
const ensureIndexes = async(ES: ESClient, workIndexName: string, cloudtrailIndexName: string) => {
    const d = debug("ensureIndexes");
    const indices = ES.indices;
    const [workIndex, CTIndex] =
        await Promise.all([
            indices.exists({ index: workIndexName }),
            indices.exists({ index: cloudtrailIndexName }),
        ]);

    const makeWorkIndex = () =>
        ES.indices.create({
            index: cloudtrailIndexName,
            body: {
                _doc: {
                    properties: {
                        eventTime: {type: "date", format: "date_time_no_millis"}
                    }
                }
            },
        });

    const makeCTIndex = () =>
        ES.indices.create({
            index: cloudtrailIndexName,
            body: {
                _doc: {
                    properties: {
                        eventTime: {type: "date", format: "date_time_no_millis"}
                    }
                }
            },
        });

    await Promise.all([
        workIndex.body ? makeWorkIndex() : d.log(`${workIndexName} exists`),
        CTIndex.body ? makeCTIndex() : d.log(`${cloudtrailIndexName} exists`),
    ]);
};

const generateS3Objs = async function*(): AsyncGenerator<Object, void> {
    const sourceS3 = new AWS.S3({region: program.region});
    const d = debug("generateS3Objs");

    const params: ListObjectsRequest = {Bucket: program.bucket, Prefix: program.prefix};

    try {
        let objs: ListObjectsOutput;
        do {
            /* fetch a list of all the objects to be processed based on the prefix*/
            d("Fetching %s with marker: %s", program.prefix, params.Marker);
            objs = await sourceS3.listObjects(params).promise();
            if (objs.Contents) {
                for(const obj of objs.Contents) {
                    yield obj;
                }
                if(objs.IsTruncated) {
                    params.Marker = objs.NextMarker;
                }
            }
        } while(objs.IsTruncated);
    } catch(e) {
        d("ERROR: %s", e);
        throw e;
    }
};

const processItems = async (objs: AsyncIterator<Object, void>) => {
    d.info("Processing S3 Objects");

    const S3 = new AWS.S3({region: program.region});
    const processors: Promise<void>[] = [];
    const processItem = createItemProcessor(S3);

    for(let i = 0; i < ES_FILE_CONCURRENCY; i++) {
        processors.push(consumeAll(objs, processItem))
    }

    await Promise.all(processors);
};

const consumeAll = async <T>(list: AsyncIterator<T, void>, processor: (item: T) => Promise<unknown>) => {
    while(true) {
        const result = await list.next();
        if (result.done) {
            break
        }
        const current: T = result.value;
        await processor(current);
    }
};

const createItemProcessor = (S3: AWS.S3) => async (obj: Object) => {
    if (!obj.Key) {
        d.ESError(`${obj} has no Key! This should not happen.`);
        return
    }

    const id: string = createSigner()
        .update(obj.Key)
        .update(program.bucket)
        .digest()
        .toString("hex");

    try {
        const res = await ES.get({ index: program.workIndex, id });
        if(res.statusCode === 200) {
            d.info(`skip ${obj.Key}, already exists`);
            return
        }
        if(res.statusCode !== 200 && res.statusCode !== 404) {
            d.ESError(res.body);
            return
        }
        d.info(`Processing ${obj.Key}`);

        const stream = S3.getObject({
            Bucket: program.bucket,
            Key: obj.Key,
        }).createReadStream()
            .pipe(zlib.createGunzip());

        let jsonSrc = "";
        for await (const data of stream) {
            jsonSrc += data.toString();
        }

        const json = JSON.parse(jsonSrc);

        const bulk = json.Records.map((record: {}) => ({
            index: {
                index: program.cloudtrailIndex,
                data: record,
            },
        }));

        const doc = {
            id,
            key: obj.Key,
            timestamp: moment().format()
        };

        bulk.push({
            index: {
                index: program.workIndex,
                data: doc,
                id
            }
        });

        await ES.bulk(bulk);
    } catch(e) {
        d.ESError(`Error importing ${obj.Key}: %s`, e);
        // Continue processing in case of error.
    }
};

