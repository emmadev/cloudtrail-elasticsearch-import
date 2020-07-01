import * as AWS from "aws-sdk";
import * as ES from "@elastic/elasticsearch";
import {createS3Fake} from "../fakes/s3.fake";
import {createESFake} from "../fakes/elasticsearch.fake";
import {_private, cloudtrailLogRecordExtractor} from "../../../import/extraction/cloudtrail-log-records";
import * as zlib from "zlib";
import * as streamifier from "streamifier";
import {Merge} from "../../../import/common/merge";

describe("Cloudtrail Extractor", () => {
    const takeAll = async <T>(asyncIterable: AsyncIterable<T>): Promise<T[]> => {
        const array: T[] = [];
        for await(const element of asyncIterable) {
            array.push(element);
        }
        return array;
    };

    describe("listS3Objs", () => {
        let s3: AWS.S3;
        const bucket = "test-bucket";
        const prefix = "/test-prefix";
        beforeEach(async () => {
            s3 = createS3Fake();
            await s3.createBucket({Bucket: bucket}).promise();
        });
        it("gets all objects at '$bucket/$prefix' and nothing else", async () => {
            for(let i = 0; i < 2001; i++) {
                await s3.putObject({
                    Bucket: bucket,
                    Key: `${prefix}/obj-${i}`,
                    Body: 'a',
                }).promise();
            }
            await s3.putObject({
                Bucket: bucket,
                Key: `/test-prefiks/obj-z`,
                Body: 'a',
            }).promise();
            await s3.createBucket({Bucket: "tezt-bucket"}).promise();
            await s3.putObject({
                Bucket: "tezt-bucket",
                Key: `${prefix}/obj-z`,
                Body: 'a',
            }).promise();

            const objs = await takeAll(_private.listS3Objs(s3, bucket, prefix));

            for (let i = 0; i < 2001; i++) {
                expect(objs.some(obj => obj.Key === `${prefix}/obj-${i}`)).toBeTruthy();
            }
            expect(objs.some(obj => obj.Key === `/test-prefiks/obj-z`)).toBeFalsy();
            expect(objs.some(obj => obj.Key === `${prefix}/obj-z`)).toBeFalsy();
        })
    });

    describe("alreadyFinished/markFinished", () => {
        let es: ES.Client;
        const bucket = "test-bucket";
        const key = "/test-prefix/obj-1";
        const workIndex = "work-index";
        beforeEach(async () => {
            es = createESFake();
            await es.indices.create({
                index: workIndex,
            });
        });

        it("alreadyFinished returns false if the object has not been marked finished", async() => {
            const finished = await _private.alreadyFinished(es, workIndex, bucket, key);
            expect(finished).toBeFalsy();
        });

        it("alreadyFinished returns true if the object has been marked finished", async() => {
            await _private.markFinished(es, workIndex, bucket, key);
            const finished = await _private.alreadyFinished(es, workIndex, bucket, key);
            expect(finished).toBeTruthy();
        });
    });

    describe("readGzipS3Stream", () => {
        let s3: AWS.S3;
        const bucket = "test-bucket";
        const key = "/test-prefix/obj-1";
        beforeEach(async () => {
            s3 = createS3Fake();
            await s3.createBucket({Bucket: bucket}).promise();
        });
        it("Unzips zipped data", async () => {
            await s3.putObject({
                Bucket: bucket,
                Key: key,
                Body: streamifier.createReadStream(Buffer.alloc(9, "Test data")).pipe(zlib.createGzip())
            }).promise();
            const readable = _private.readGzipS3Stream(s3, bucket, key);
            let body = "";
            for await(const data of readable) {
                body += data;
            }
            expect(body).toStrictEqual("Test data");
        });
    });

    describe("readAll", () => {
        it("Reads all the data from a stream", async () => {
            let bigdata = "";
            for (let i = 0; i < 100_000; i++) {
                bigdata += `${i}`;
            }
            const readable = streamifier.createReadStream(Buffer.alloc(bigdata.length, bigdata));
            expect(await _private.readAll(readable)).toStrictEqual(bigdata);
        })
    });

    describe("parseCloudtrailLog", () => {
        it("Parses cloudtrail log entries", () => {
            const logRecords = {
                Records: [
                    {
                        a: "b",
                        c: "d",
                    },
                    {
                        e: "f",
                        g: "h",
                    },
                ]
            };
            const cloudtrailLog = _private.parseCloudtrailLog("key",JSON.stringify(logRecords));
            expect(cloudtrailLog).toStrictEqual(logRecords);
        });

        it("returns null for malformed JSON", () => {
            const cloudtrailLog = _private.parseCloudtrailLog("key", "{gar[bage;;");
            expect(cloudtrailLog).toBeNull();
        });

        it("returns null for schema mismatch", () => {
            const badSchema1 = {
                Wreckers: [
                    {
                        a: "b",
                        c: "d",
                    },
                    {
                        e: "f",
                        g: "h",
                    },
                ]
            };
            const badSchema2 = {
                Records: [
                    {
                        a: "b",
                        c: "d",
                    },
                    {
                        userIdentity: "shouldn't be a string here",
                    },
                ]
            };
            const cloudtrailLog1 = _private.parseCloudtrailLog("key", JSON.stringify(badSchema1));
            expect(cloudtrailLog1).toBeNull();
            const cloudtrailLog2 = _private.parseCloudtrailLog("key", JSON.stringify(badSchema2));
            expect(cloudtrailLog2).toBeNull();
        })
    });

    describe("ensureWorkIndex", () => {
        let es: ES.Client;
        const workIndex = "work-index";
        beforeEach(async () => {
            es = createESFake();
        });
        it("Creates the work index if it does not exist", async () => {
            await _private.ensureWorkIndex(es, workIndex);
            expect(await es.indices.exists({ index: workIndex })).toBeTruthy();
        });
        it("Does not error if work index does exist", async () => {
            await es.indices.create({ index: workIndex });
            await expect(_private.ensureWorkIndex(es, workIndex)).resolves.not.toThrow();
        });
    });

    describe("eachRecord", () => {
        describe("returns an AsyncIterable that", () => {
            let s3: AWS.S3;
            const bucket = "test-bucket";
            let es: ES.Client;
            const workIndex = "work-index";
            const key = "/test-prefix/obj-3";
            beforeEach(async () => {
                s3 = createS3Fake();
                es = createESFake();
                await _private.ensureWorkIndex(es, workIndex);
                const jsonBody = JSON.stringify({
                    Records: [
                        {
                            a: "b",
                            c: "d",
                        },
                        {
                            e: "f",
                            g: "h",
                        },
                    ]
                });
                await s3.putObject({
                    Bucket: bucket,
                    Key: key,
                    Body: streamifier
                        .createReadStream(
                            Buffer.alloc(jsonBody.length, jsonBody)
                        )
                        .pipe(zlib.createGzip()),
                }).promise();
            });
            it("yields every record in the S3 object", async () => {
                const asyncIterator = _private.eachRecord(s3, es, workIndex, bucket)({Key: key})[Symbol.asyncIterator]();

                expect(await asyncIterator.next()).toStrictEqual({done: false, value: {a: "b", c: "d"}});
                expect(await asyncIterator.next()).toStrictEqual({done: false, value: {e: "f", g: "h"}});
                expect((await asyncIterator.next()).done).toBeTruthy();
            });
            it("yields nothing if the object has already been imported", async () => {
                await _private.markFinished(es, workIndex, bucket, key);
                const asyncIterator = _private.eachRecord(s3, es, workIndex, bucket)({Key: key})[Symbol.asyncIterator]();

                expect((await asyncIterator.next()).done).toBeTruthy();
            });
            it("yields nothing if the object has no key", async () => {
                const asyncIterator = _private.eachRecord(s3, es, workIndex, bucket)({})[Symbol.asyncIterator]();

                expect((await asyncIterator.next()).done).toBeTruthy();
            });
        });
    });

    describe("cloudtrailLogRecordExtractor", () => {
        const sequentialMerge: Merge = <T, U>(f: (t: T) => AsyncIterable<U>) => async function*(ait: AsyncIterable<T>): AsyncIterable<U> {
            for await(const t of ait) {
                for await(const u of f(t)) {
                    yield u;
                }
            }
        };

        let s3: AWS.S3;
        const bucket = "test-bucket";
        let es: ES.Client;
        const workIndex = "work-index";
        const prefix = "/test-prefix";

        beforeEach(async () => {
            s3 = createS3Fake();
            es = createESFake();
            await _private.ensureWorkIndex(es, workIndex);
            const jsonBody1 = JSON.stringify({
                Records: [
                    {
                        a: "b",
                        c: "d",
                    },
                    {
                        e: "f",
                        g: "h",
                    },
                ]
            });
            const jsonBody2 = JSON.stringify({
                Records: [
                    {
                        i: "j",
                        k: "l",
                    },
                    {
                        m: "n",
                        o: "p",
                    },
                ]
            });
            const jsonBody3 = JSON.stringify({
                Records: [
                    {
                        q: "r",
                        s: "t",
                    },
                    {
                        u: "v",
                        w: "x",
                    },
                ]
            });
            await s3.putObject({
                Bucket: bucket,
                Key: `${prefix}/obj-1`,
                Body: streamifier
                    .createReadStream(
                        Buffer.alloc(jsonBody1.length, jsonBody1)
                    )
                    .pipe(zlib.createGzip()),
            }).promise();
            await s3.putObject({
                Bucket: bucket,
                Key: `${prefix}/obj-2`,
                Body: streamifier
                    .createReadStream(
                        Buffer.alloc(jsonBody2.length, jsonBody2)
                    )
                    .pipe(zlib.createGzip()),
            }).promise();
            await s3.putObject({
                Bucket: bucket,
                Key: `${prefix}/obj-3`,
                Body: streamifier
                    .createReadStream(
                        Buffer.alloc(jsonBody3.length, jsonBody3)
                    )
                    .pipe(zlib.createGzip()),
            }).promise();
        });

        const extractor = cloudtrailLogRecordExtractor(sequentialMerge);

        describe("returns an async function that", () => {
            it("Ensures the workIndex is set", async () => {
                const asyncIterator = extractor({workIndex, bucket, prefix, s3, es})()[Symbol.asyncIterator]();
                await asyncIterator.next();
                expect((await es.indices.exists({ index: workIndex })).body).toBeTruthy();
            });
            describe("returns an AsyncIterable that", () => {
                it("yields all records in all objects at the prefix", async () => {
                    const asyncIterator = extractor({workIndex, bucket, prefix, s3, es})()[Symbol.asyncIterator]();
                    expect(await asyncIterator.next()).toStrictEqual({done: false, value: {a: "b", c: "d"}});
                    expect(await asyncIterator.next()).toStrictEqual({done: false, value: {e: "f", g: "h"}});
                    expect(await asyncIterator.next()).toStrictEqual({done: false, value: {i: "j", k: "l"}});
                    expect(await asyncIterator.next()).toStrictEqual({done: false, value: {m: "n", o: "p"}});
                    expect(await asyncIterator.next()).toStrictEqual({done: false, value: {q: "r", s: "t"}});
                    expect(await asyncIterator.next()).toStrictEqual({done: false, value: {u: "v", w: "x"}});
                    expect((await asyncIterator.next()).done).toBeTruthy();
                });
                it("yields no records if the prefix does not exist", async () => {
                    const asyncIterator = extractor({workIndex, bucket, prefix: "/missing-prefix", s3, es})()[Symbol.asyncIterator]();
                    expect((await asyncIterator.next()).done).toBeTruthy();
                });
                it("marks each object complete as it finishes", async () => {
                    const asyncIterator = extractor({workIndex, bucket, prefix, s3, es})()[Symbol.asyncIterator]();
                    await asyncIterator.next();
                    await asyncIterator.next();

                    expect(await _private.alreadyFinished(es, workIndex, bucket, `${prefix}/obj-1`)).toBeFalsy();
                    await asyncIterator.next();
                    expect(await _private.alreadyFinished(es, workIndex, bucket, `${prefix}/obj-1`)).toBeTruthy();
                    await asyncIterator.next();

                    expect(await _private.alreadyFinished(es, workIndex, bucket, `${prefix}/obj-2`)).toBeFalsy();
                    await asyncIterator.next();
                    expect(await _private.alreadyFinished(es, workIndex, bucket, `${prefix}/obj-2`)).toBeTruthy();
                    await asyncIterator.next();

                    expect(await _private.alreadyFinished(es, workIndex, bucket, `${prefix}/obj-3`)).toBeFalsy();
                    await asyncIterator.next();
                    expect(await _private.alreadyFinished(es, workIndex, bucket, `${prefix}/obj-3`)).toBeTruthy();
                });
                it("doesn't extract objects on subsequent runs", async () => {
                    const asyncIterator1 = extractor({workIndex, bucket, prefix, s3, es})()[Symbol.asyncIterator]();
                    for(let i = 0; i < 7; i++) {
                        await asyncIterator1.next();
                    }
                    const jsonBody4 = JSON.stringify({
                        Records: [
                            {
                                y: "z",
                            },
                        ],
                    });
                    await s3.putObject({
                        Bucket: bucket,
                        Key: `${prefix}/obj-4`,
                        Body: streamifier
                            .createReadStream(
                                Buffer.alloc(jsonBody4.length, jsonBody4)
                            )
                            .pipe(zlib.createGzip()),
                    }).promise();
                    const asyncIterator2 = extractor({workIndex, bucket, prefix, s3, es})()[Symbol.asyncIterator]();
                    expect(await asyncIterator2.next()).toStrictEqual({done: false, value: {y: "z"}});
                    expect((await asyncIterator2.next()).done).toBeTruthy();
                });
            });
        });
    });
});
