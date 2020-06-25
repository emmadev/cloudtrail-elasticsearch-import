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

import AWS from 'aws-sdk';
import { URL } from 'url';
import { Client as ESClient } from '@elastic/elasticsearch';
import debug from 'debug';
import { version } from './package.json';

import {Program} from "./src/types/input/program";
import {Env} from "./src/types/input/environment";

import {cloudtrailLogRecordExtractor} from "./src/extraction/cloudtrail-log-records";
import {convertCloudtrailToElasticsearch} from "./src/transformation/cloudtrail-to-elasticsearch";
import {elasticsearchLogRecordLoader} from "./src/load/elasticsearch-log-records";
import {batch} from "./src/common/batch";

const d = {
    ESError: debug("ElasticSearch:error"),
    info: debug("info"),
    error: debug("error")
};

const program: Program = (() => {
    const { validateProgram } = require('./src/types/input/program');
    const commander = require('commander');

    commander
        .version(version)
        .option('-b, --bucket <sourcebucket>', 'Bucket with cloudtrail logs', String, '')
        .option('-r, --region <bucket region>', 'Default region: us-east-1', String, 'us-east-1')
        .option('-p, --prefix <prefix>', 'prefix where to start listing objects')

        .option('-c, --parallelism <#>', 'number of concurrent workers, default: 5', parseInt, 5)
        .option('-s, --batch-size <#>', 'batch import size, default: 10000', parseInt, 10_000)

        .option('-e, --elasticsearch <url>', 'ES base, ie: https://host:port', String, '')
        .option('--work-index <name>', 'ES index to record imported files, def: cloudtrail-imported', String, 'cloudtrail-import-log')
        .option('--cloudtrail-index <name>', 'ES index to put cloudtrail events, def: cloudtrail', String, 'cloudtrail')
        .parse(process.argv);

    return validateProgram(commander) || process.exit(1);
})();

const env: Env = (() => {
    const { validateEnvironment } = require('./src/types/input/environment');
    return validateEnvironment(process.env) || process.exit(1);
})();

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

const S3 = new AWS.S3({
    region: program.region,
    accessKeyId: env.AWS_ACCESS_KEY,
    secretAccessKey: env.AWS_SECRET_KEY
});

const batchLogImport = batch(program.parallelism, program.batchSize)(
    cloudtrailLogRecordExtractor,
    convertCloudtrailToElasticsearch,
    elasticsearchLogRecordLoader
);

const run = async () => {
    await batchLogImport({
        program, es: ES, s3: S3
    }, {
        program, es: ES
    });
};

// Run forever until explicitly closed with a `process.exit()`
setInterval(() => {}, 0x7FFFFFFF);

run()
    .then(() => process.exit(0))
    .catch(e => {
        d.error("UNCAUGHT ERROR: %s", e);
        process.exit(255);
    });
