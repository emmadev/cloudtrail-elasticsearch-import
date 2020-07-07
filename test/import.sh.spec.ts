import AWS from 'aws-sdk';
import {Client as ESClient} from '@elastic/elasticsearch';
import {createS3Fake} from "./import/fakes/s3.fake";
import {createESFake} from "./import/fakes/elasticsearch.fake";
import {CloudtrailLog} from "../import/types/log/cloudtrail-log";
import * as streamifier from "streamifier";
import * as zlib from "zlib";

describe("import.sh.ts", () => {
    const processArgvOriginal = process.argv;
    const processEnvOriginal = process.env;

    let s3Fake: AWS.S3;
    let esFake: ESClient;

    const mockExternals = () => {
        jest.mock('aws-sdk', () => ({
            S3: jest.fn().mockImplementation(() => s3Fake)
        }));
        jest.mock('@elastic/elasticsearch', () => ({
            Client: jest.fn().mockImplementation(() => esFake)
        }));
    };

    beforeEach(() => {
        jest.setTimeout(30_000);
        process.argv = [];
        process.env = {};

        s3Fake = createS3Fake();
        esFake = createESFake();
        mockExternals();
    });

    it("Imports all records from all logs at the given prefix in the given bucket", async () => {
        const bucketName = "cloudtrail-bucket";
        const prefix = "log-prefix";
        const workIndex = "work-index";
        const cloudtrailIndex = "cloudtrail-index";

        await s3Fake.createBucket({ Bucket: bucketName }).promise();
        const log1: CloudtrailLog = {
            Records: [
                {
                    id: "a",
                    a: 1,
                    userIdentity: {
                        b: 2,
                        sessionContext: {
                            c: 3,
                        }
                    },
                    requestParameters: {
                        d: 4,
                    },
                    responseElements: {
                        e: 5,
                    }
                },
                {
                    id: "b",
                    f: 1,
                    userIdentity: {
                        g: 2,
                    },
                    requestParameters: {
                        h: 3,
                    },
                    responseElements: {
                        i: 4,
                    }
                },
                {
                    id: "c",
                    j: 1,
                    requestParameters: {
                        k: 4,
                    },
                    responseElements: {
                        l: 5,
                    }
                },
            ]
        };
        const log2: CloudtrailLog = {
            Records: [
                {
                    id: "d",
                    m: 1,
                    userIdentity: {
                        n: 2,
                        sessionContext: {
                            o: 3,
                        }
                    },
                    responseElements: {
                        p: 5,
                    }
                },
                {
                    id: "e",
                    q: 1,
                    userIdentity: {
                        r: 2,
                        sessionContext: {
                            s: 3,
                        }
                    },
                    requestParameters: {
                        t: 3,
                    },
                },
            ]
        };
        await s3Fake.putObject({
            Bucket: bucketName,
            Key: `${prefix}/log1`,
            Body: streamifier.createReadStream(Buffer.alloc(
                JSON.stringify(log1).length,
                JSON.stringify(log1),
            )).pipe(zlib.createGzip()),
        }).promise();
        await s3Fake.putObject({
            Bucket: bucketName,
            Key: `${prefix}/log2`,
            Body: streamifier.createReadStream(Buffer.alloc(
                JSON.stringify(log2).length,
                JSON.stringify(log2),
            )).pipe(zlib.createGzip()),
        }).promise();

        process.argv = [
            "node", "import.sh.ts",
            "-b", bucketName,
            "-r", "us-east-1",
            "-p", prefix,
            "-e", "https://fakehost:9200",
            "--work-index", workIndex,
            "--cloudtrail-index", cloudtrailIndex,
        ];

        process.env = {
            AWS_ACCESS_KEY: "AAAAAAAAAAAAAAAAAAAA",
            AWS_SECRET_KEY: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa==",
        };

        jest.useFakeTimers();
        const systemExit = new Promise(resolve => {
            jest.spyOn(process, 'exit')
                // @ts-ignore
                .mockImplementation(code => resolve(code));
        });

        require('../import.sh');
        const exitCode = await systemExit;

        expect(exitCode).toStrictEqual(0);

        expect((await esFake.get({
            index: cloudtrailIndex,
            id: "a",
        })).body._source).toStrictEqual({
            id: "a",
            a: 1,
            userIdentity: {
                b: 2,
                sessionContext: '{"c":3}',
            },
            requestParameters: '{"d":4}',
            responseElements: '{"e":5}',
            raw: '{"id":"a","a":1,"userIdentity":{"b":2,"sessionContext":{"c":3}},"requestParameters":{"d":4},"responseElements":{"e":5}}',
        });
        expect((await esFake.get({
            index: cloudtrailIndex,
            id: "b",
        })).body._source).toStrictEqual({
            id:"b",
            f: 1,
            userIdentity: {
                g: 2,
            },
            requestParameters: '{"h":3}',
            responseElements: '{"i":4}',
            raw: "{\"id\":\"b\",\"f\":1,\"userIdentity\":{\"g\":2},\"requestParameters\":{\"h\":3},\"responseElements\":{\"i\":4}}",
        });
        expect((await esFake.get({
            index: cloudtrailIndex,
            id: "c",
        })).body._source).toStrictEqual({
            id: "c",
            j: 1,
            requestParameters: '{"k":4}',
            responseElements: '{"l":5}',
            raw: '{"id":"c","j":1,"requestParameters":{"k":4},"responseElements":{"l":5}}',
        });
        expect((await esFake.get({
            index: cloudtrailIndex,
            id: "d",
        })).body._source).toStrictEqual({
            id: "d",
            m: 1,
            userIdentity: {
                n: 2,
                sessionContext: '{"o":3}',
            },
            responseElements: '{"p":5}',
            raw: '{"id":"d","m":1,"userIdentity":{"n":2,"sessionContext":{"o":3}},"responseElements":{"p":5}}',
        });
        expect((await esFake.get({
            index: cloudtrailIndex,
            id: "e",
        })).body._source).toStrictEqual({
            id: "e",
            q: 1,
            userIdentity: {
                r: 2,
                sessionContext: '{"s":3}',
            },
            requestParameters: '{"t":3}',
            raw: '{"id":"e","q":1,"userIdentity":{"r":2,"sessionContext":{"s":3}},"requestParameters":{"t":3}}',
        });
    });

    it("Doesn't reimport a log if it hasn't changed", async () => {
        const bucketName = "cloudtrail-bucket";
        const prefix = "log-prefix";
        const workIndex = "work-index";
        const cloudtrailIndex = "cloudtrail-index";

        await s3Fake.createBucket({Bucket: bucketName}).promise();
        const log1: CloudtrailLog = {
            Records: [
                {
                    id: "a",
                    a: 1,
                    userIdentity: {
                        b: 2,
                        sessionContext: {
                            c: 3,
                        }
                    },
                    requestParameters: {
                        d: 4,
                    },
                    responseElements: {
                        e: 5,
                    }
                },
            ]
        };
        await s3Fake.putObject({
            Bucket: bucketName,
            Key: `${prefix}/log1`,
            Body: streamifier.createReadStream(Buffer.alloc(
                JSON.stringify(log1).length,
                JSON.stringify(log1),
            )).pipe(zlib.createGzip()),
        }).promise();

        process.argv = [
            "node", "import.sh.ts",
            "-b", bucketName,
            "-r", "us-east-1",
            "-p", prefix,
            "-e", "https://fakehost:9200",
            "--work-index", workIndex,
            "--cloudtrail-index", cloudtrailIndex,
        ];

        process.env = {
            AWS_ACCESS_KEY: "AAAAAAAAAAAAAAAAAAAA",
            AWS_SECRET_KEY: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa==",
        };

        jest.useFakeTimers();
        let systemExit = new Promise(resolve => {
            jest.spyOn(process, 'exit')
                // @ts-ignore
                .mockImplementation(code => resolve(code));
        });

        require('../import.sh');
        let exitCode = await systemExit;
        expect(exitCode).toStrictEqual(0);

        await esFake.delete({ index: cloudtrailIndex, id: "a" });
        jest.resetModules();
        mockExternals();
        jest.useFakeTimers();
        systemExit = new Promise(resolve => {
            jest.spyOn(process, 'exit')
                // @ts-ignore
                .mockImplementation(code => resolve(code));
        });

        require('../import.sh');
        exitCode = await systemExit;
        expect(exitCode).toStrictEqual(0);

        expect((await esFake.get({
            index: cloudtrailIndex,
            id: "a",
        }, {
            ignore: [404],
        })).statusCode).toStrictEqual(404);
    });

    it("Reimports a log if it has changed", async () => {
        const bucketName = "cloudtrail-bucket";
        const prefix = "log-prefix";
        const workIndex = "work-index";
        const cloudtrailIndex = "cloudtrail-index";

        await s3Fake.createBucket({Bucket: bucketName}).promise();
        const log1a: CloudtrailLog = {
            Records: [
                {
                    id: "a",
                    a: 1,
                    userIdentity: {
                        b: 2,
                        sessionContext: {
                            c: 3,
                        }
                    },
                    requestParameters: {
                        d: 4,
                    },
                    responseElements: {
                        e: 5,
                    }
                },
            ]
        };
        const log1b: CloudtrailLog = {
            Records: [
                {
                    id: "a",
                    a: 1,
                    userIdentity: {
                        b: 2,
                        sessionContext: {
                            c: 3,
                        }
                    },
                    requestParameters: {
                        d: 4,
                    },
                    responseElements: {
                        e: 5,
                    }
                },
                {
                    id: "b",
                    f: 1,
                    userIdentity: {
                        g: 2,
                    },
                    requestParameters: {
                        h: 3,
                    },
                    responseElements: {
                        i: 4,
                    }
                },
            ]
        };
        await s3Fake.putObject({
            Bucket: bucketName,
            Key: `${prefix}/log1`,
            Body: streamifier.createReadStream(Buffer.alloc(
                JSON.stringify(log1a).length,
                JSON.stringify(log1a),
            )).pipe(zlib.createGzip()),
        }).promise();

        process.argv = [
            "node", "import.sh.ts",
            "-b", bucketName,
            "-r", "us-east-1",
            "-p", prefix,
            "-e", "https://fakehost:9200",
            "--work-index", workIndex,
            "--cloudtrail-index", cloudtrailIndex,
        ];

        process.env = {
            AWS_ACCESS_KEY: "AAAAAAAAAAAAAAAAAAAA",
            AWS_SECRET_KEY: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa==",
        };

        jest.useFakeTimers();
        let systemExit = new Promise(resolve => {
            jest.spyOn(process, 'exit')
                // @ts-ignore
                .mockImplementation(code => resolve(code));
        });

        require('../import.sh');
        let exitCode = await systemExit;
        expect(exitCode).toStrictEqual(0);

        await s3Fake.putObject({
            Bucket: bucketName,
            Key: `${prefix}/log1`,
            Body: streamifier.createReadStream(Buffer.alloc(
                JSON.stringify(log1b).length,
                JSON.stringify(log1b),
            )).pipe(zlib.createGzip()),
        }).promise();

        jest.resetModules();
        mockExternals();
        jest.useFakeTimers();
        systemExit = new Promise(resolve => {
            jest.spyOn(process, 'exit')
                // @ts-ignore
                .mockImplementation(code => resolve(code));
        });

        require('../import.sh');
        exitCode = await systemExit;
        expect(exitCode).toStrictEqual(0);

        expect((await esFake.get({
            index: cloudtrailIndex,
            id: "a",
        })).body._source).toStrictEqual({
            id: "a",
            a: 1,
            userIdentity: {
                b: 2,
                sessionContext: '{"c":3}',
            },
            requestParameters: '{"d":4}',
            responseElements: '{"e":5}',
            raw: '{"id":"a","a":1,"userIdentity":{"b":2,"sessionContext":{"c":3}},"requestParameters":{"d":4},"responseElements":{"e":5}}',
        });
        expect((await esFake.get({
            index: cloudtrailIndex,
            id: "b",
        })).body._source).toStrictEqual({
            id: "b",
            f: 1,
            userIdentity: {
                g: 2,
            },
            requestParameters: '{"h":3}',
            responseElements: '{"i":4}',
            raw: "{\"id\":\"b\",\"f\":1,\"userIdentity\":{\"g\":2},\"requestParameters\":{\"h\":3},\"responseElements\":{\"i\":4}}",
        });
    });

    afterEach(() => {
        process.argv = processArgvOriginal;
        process.env = processEnvOriginal;
        jest.restoreAllMocks();
        jest.useRealTimers();
        jest.setTimeout(5_000);
        jest.resetModules();
    });

});
