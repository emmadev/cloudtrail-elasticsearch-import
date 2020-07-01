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

import {cloudtrailLogRecordExtractor} from "./import/extraction/cloudtrail-log-records";
import {convertCloudtrailToElasticsearch} from "./import/transformation/cloudtrail-to-elasticsearch";
import {elasticsearchLogRecordLoader} from "./import/load/elasticsearch-log-records";
import {batch} from "./import/common/batch";

const d = {
    ESError: debug("ElasticSearch:error"),
    info: debug("info"),
    error: debug("error")
};

const parseProgram = async () => {
    const { validateProgram } = require('./import/types/input/program');
    const tcl = require('@rushstack/ts-command-line');
    const parser = new tcl.DynamicCommandLineParser({
        toolFilename: "import.sh.ts",
        toolDescription: "A simple ETL script for transferring CloudTrail logs from S3 to Elasticsearch",
    });

    parser.defineFlagParameter({
        parameterLongName: "--version",
        parameterShortName: "-v",
        description: "Display the version number and exit.",
    });

    parser.defineStringParameter({
        parameterLongName: "--bucket",
        parameterShortName: "-b",
        argumentName: "source bucket",
        description: "Bucket with cloudtrail logs",
    });
    parser.defineStringParameter({
        parameterLongName: "--region",
        parameterShortName: "-r",
        argumentName: "bucket region",
        defaultValue: "us-east-1",
    });
    parser.defineStringParameter({
        parameterLongName: "--prefix",
        parameterShortName: "-p",
        argumentName: "prefix",
        description: "prefix to use when listing S3 objects",
    });

    parser.defineIntegerParameter({
        parameterLongName: "--parallelism",
        parameterShortName: "-w",
        argumentName: "workers",
        description: "number of concurrent workers",
        defaultValue: 5,
    });
    parser.defineIntegerParameter({
        parameterLongName: "--batch-size",
        parameterShortName: "-s",
        argumentName: "records",
        description: "max number of records in a batch",
        defaultValue: 10_000,
    });

    parser.defineStringParameter({
        parameterLongName: "--elasticsearch",
        parameterShortName: "-e",
        argumentName: "url",
        description: "Elasticsearch base, e.g. https://host:9200",
    });
    parser.defineStringParameter({
        parameterLongName: "--work-index",
        argumentName: "name",
        description: "Elasticsearch index to record imported files",
        defaultValue: "cloudtrail-import-log",
    });
    parser.defineStringParameter({
        parameterLongName: "--cloudtrail-index",
        argumentName: "name",
        description: "Elasticsearch index to add cloudtrail events to",
        defaultValue: "cloudtrail",
    });

    parser.defineStringParameter({
        environmentVariable: "AWS_ACCESS_KEY",
        argumentName: "key",
        description: "AWS Access (public) Key",
    });
    parser.defineStringParameter({
        environmentVariable: "AWS_SECRET_KEY",
        argumentName: "secret",
        description: "AWS Secret (private) Key",
    });

    await parser.execute(process.argv);

    if(parser.version) {
        console.log(version);
        process.exit(1);
    }

    return validateProgram(parser) || process.exit(1);
};

const batchLogImport = batch(
    cloudtrailLogRecordExtractor,
    convertCloudtrailToElasticsearch,
    elasticsearchLogRecordLoader
);

const run = async () => {
    const program = await parseProgram();

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
