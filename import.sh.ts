#!/usr/bin/env -S npx ts-node

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

import {cloudtrailLogRecordExtractor} from "./import/extraction/cloudtrail-log-records";
import {convertCloudtrailToElasticsearch} from "./import/transformation/cloudtrail-to-elasticsearch";
import {elasticsearchLogRecordLoader} from "./import/load/elasticsearch-log-records";
import {batch} from "./import/common/batch";
import {Program} from "./import/types/input/program";

const d = {
    ESError: debug("ElasticSearch:error"),
    info: debug("info"),
    error: debug("error")
};

const parseProgram = async () => {
    const { validateProgram } = require('./import/types/input/program');
    const args = require('yargs')
        .env()
        .option('--version', {
            alias: '-v',
            describe: 'Display the version number and exit.',
            type: 'boolean',
        })
        .option('--bucket', {
            alias: '-b',
            describe: 'Bucket with cloudtrail logs',
            demandOption: true,
            type: 'string',
        })
        .option('--region', {
            alias: '-r',
            describe: 'The AWS region to use',
            default: 'us-east-1',
            type: 'string',
        })
        .option('--prefix', {
            alias: '-p',
            describe: 'The S3 prefix to search for logs',
            default: '',
            type: 'string',
        })
        .option('--parallelism', {
            alias: '-w',
            describe: 'Number of concurrent workers',
            default: 5,
            type: 'number',
        })
        .option('--batch-size', {
            alias: '-s',
            describe: 'Max number of records per batch',
            default: 1_000,
            type: 'number',
        })
        .option('--elasticsearch', {
            alias: '-e',
            describe: 'Elasticsearch base, e.g. https://host:9200',
            demandOption: true,
            type: 'string',
        })
        .option('--work-index', {
            describe: 'Elasticsearch index to record imported logs',
            default: 'cloudtrail-import-log',
            type: 'string',
        })
        .option('--cloudtrail-index', {
            describe: 'Elasticsearch index to import into',
            default: 'cloudtrail',
            type: 'string',
        })
        .option('--aws-access-key', {
            alias: 'AWS_ACCESS_KEY',
            describe: 'AWS Access Key (public)',
            type: 'string',
        })
        .option('--aws-secret-key', {
            alias: 'AWS_SECRET_KEY',
            describe: 'AWS Secret Key (private)',
            type: 'string',
        })
        .help()
        .argv;

    if(args.version) {
        console.log(version);
        process.exit(1);
    }

    return validateProgram(args) || process.exit(1);
};

const batchLogImport = batch(
    cloudtrailLogRecordExtractor,
    convertCloudtrailToElasticsearch,
    elasticsearchLogRecordLoader
);

const run = async () => {
    const program: Program = await parseProgram();

    const ES = (() => {
        const esUrl = new URL(program.elasticsearch);
        esUrl.port = esUrl.port || '9200';
        return new ESClient({
            node: {
                url: esUrl,
            },
        });
    })();

    const S3 = new AWS.S3({
        region: program.region,
        accessKeyId: program.AWS_ACCESS_KEY,
        secretAccessKey: program.AWS_SECRET_KEY,
    });

    await batchLogImport(program.parallelism, program.batchSize)({
        es: ES,
        workIndex: program.workIndex,
        s3: S3,
        bucket: program.bucket,
        prefix: program.prefix,
    }, {
        es: ES,
        cloudtrailIndex: program.cloudtrailIndex,
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
