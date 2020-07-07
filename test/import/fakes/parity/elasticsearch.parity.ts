import {dir} from 'tmp';
import {spawn, ChildProcess, ChildProcessByStdio} from "child_process";
import * as ES from "@elastic/elasticsearch";
import {createESFake} from "../elasticsearch.fake";
import {Readable} from "stream";
import {URL} from "url";
import moment from "moment";

describe("Elasticsearch Fake", () => {
    let cleanup: () => void;
    let esProcess: ChildProcess;

    beforeAll(async () => {
        jest.setTimeout(600_000);
        const tmpdir = await new Promise<string>((res, rej) => {
            dir((err: unknown, path: string, cleanupCb: () => unknown) => {
                if (err) {
                    rej(err);
                } else {
                    cleanup = cleanupCb;
                    res(path);
                }
            });
        });

        const childProcess: ChildProcessByStdio<null, Readable, null> =
            spawn(
                `${__dirname}/bin/es-server.sh`,
                {
                    stdio: ["ignore", "pipe", "ignore"],
                    shell: true,
                    env: {
                        TMPDIR: tmpdir,
                        ES_VERSION: "7.7.1",
                    }
                },
            );
        esProcess = childProcess;

        await new Promise((res,rej) => {
            let output = "";
            let resolved = false;
            const startupTimer = setTimeout(() => rej("Failed to start elasticsearch server in 5 minutes"), 300_000);
            childProcess.stdout.on('data', (data: Buffer) => {
                output += data;
                if(output.match(/\[o.e.n.Node\s*][^\n]*started/)) {
                    resolved = true;
                    clearTimeout(startupTimer);
                    res();
                } else {
                    output = output.split("\n").reverse()[0];
                }
            });
            childProcess.stdout.on('end', () => {
                if(!resolved) rej("Unexpected end-of-stream");
            });
            childProcess.stdout.on('err', () => {
                if(!resolved) rej("Failed to start elasticsearch");
            });

        });
    });

    afterAll(async () => {
        jest.setTimeout(5_000);
        if(esProcess) {
            esProcess.kill("SIGINT");
            const killTimer = setTimeout(() => esProcess.kill("SIGKILL"), 15_000);
            await new Promise(res => {
                esProcess.on("close", () => {
                    res();
                });
            });
            clearTimeout(killTimer);
        }
        if(cleanup) cleanup();
    });

    const parity = <T>(name: string, fn: (s3: ES.Client) => T, timeout?: number) => {
        const realES = new ES.Client({
            node: {
                url: new URL("http://localhost:9200"),
            },
        });
        const fakeES = createESFake();

        describe(name, () => {
            it("Test Fake", () => fn(fakeES), timeout);
            it("Real Client", () => fn(realES), timeout);
        })
    };

    parity("Can create an index and check for its existence", async (es) => {
        const indexName = `testing-index-${Math.floor(Math.random() * 4294967296)}`;

        let exists = await es.indices.exists({index: indexName});
        expect(exists.body).toBeFalsy();
        await es.indices.create({
            index: indexName
        });
        exists = await es.indices.exists({index: indexName});
        expect(exists.body).toBeTruthy();
    });

    parity("Gets an error when trying to create an index that already exists", async (es) => {
        const indexName = `testing-index-${Math.floor(Math.random() * 4294967296)}`;

        await expect((async () => {
            await es.indices.create({
                index: indexName
            });
            await es.indices.create({
                index: indexName
            });
        })()).rejects.toThrow();
    });

    parity("Can index a document and see that it was indexed", async (es) => {
        const indexName = `testing-index-${Math.floor(Math.random() * 4294967296)}`;
        const documentId = `id-${Math.floor(Math.random() * 4294967296)}`;

        await es.indices.create({
            index: indexName
        });

        let statusCode = (await es.get({
            index: indexName,
            id: documentId,
        }, {
            ignore: [404],
        })).statusCode;

        expect(statusCode).toStrictEqual(404);

        await es.index({
            index: indexName,
            id: documentId,
            body: {
                abc: 123,
            }
        });

        const resp = await es.get({
            index: indexName,
            id: documentId,
        }, {
            ignore: [404],
        });

        expect(resp.statusCode).toStrictEqual(200);
        expect(resp.body._source.abc).toStrictEqual(123);
    });

    parity("Can delete a document and see that it was deleted", async (es) => {
        const indexName = `testing-index-${Math.floor(Math.random() * 4294967296)}`;
        const documentId = `id-${Math.floor(Math.random() * 4294967296)}`;

        await es.indices.create({
            index: indexName
        });

        let statusCode = (await es.get({
            index: indexName,
            id: documentId,
        }, {
            ignore: [404],
        })).statusCode;

        expect(statusCode).toStrictEqual(404);

        await es.index({
            index: indexName,
            id: documentId,
            body: {
                abc: 123,
            }
        });

        let resp = await es.delete({
            index: indexName,
            id: documentId,
        });
        expect(resp.statusCode).toStrictEqual(200);

        resp = await es.get({
            index: indexName,
            id: documentId,
        }, {
            ignore: [404],
        });

        expect(resp.statusCode).toStrictEqual(404);
    });

    parity("Can index a document with a date field and get back a moment-able value", async (es) => {
        const indexName = `testing-index-${Math.floor(Math.random() * 4294967296)}`;
        const documentId = `id-${Math.floor(Math.random() * 4294967296)}`;
        const timestamp = new Date();

        await es.indices.create({
            index: indexName,
            body: {
                mappings: {
                    properties: {
                        timestamp: {type: "date"}
                    }
                }
            }
        });

        await es.index({
            index: indexName,
            id: documentId,
            body: {
                abc: 123,
                timestamp,
            }
        });

        const resp = (await es.get({
            index: indexName,
            id: documentId,
        }, {
            ignore: [404],
        }));

        expect(resp.statusCode).toStrictEqual(200);
        expect(moment(resp.body._source.timestamp).isSame(moment(timestamp))).toBeTruthy();
    });

    parity("Can index a document with a date field (no ms) and get back a moment-able value", async (es) => {
        const indexName = `testing-index-${Math.floor(Math.random() * 4294967296)}`;
        const documentId = `id-${Math.floor(Math.random() * 4294967296)}`;
        const timestamp = new Date();
        const timestampNoMs = new Date(timestamp);
        timestampNoMs.setMilliseconds(0);

        await es.indices.create({
            index: indexName,
            body: {
                mappings: {
                    properties: {
                        timestamp: {
                            type: "date",
                            format: "date_time_no_millis",
                        }
                    }
                }
            }
        });

        await es.index({
            index: indexName,
            id: documentId,
            body: {
                abc: 123,
                timestamp: moment(timestampNoMs).format("YYYY-MM-DD[T]HH:mm:ssZ"),
            }
        });

        const resp = (await es.get({
            index: indexName,
            id: documentId,
        }, {
            ignore: [404],
        }));

        expect(resp.statusCode).toStrictEqual(200);
        expect(moment(resp.body._source.timestamp).isSame(moment(timestampNoMs))).toBeTruthy();
    });

    parity("Can't index a document with a real Date object, " +
        "if the mapping is set to no millis format", async (es) => {
        const indexName = `testing-index-${Math.floor(Math.random() * 4294967296)}`;
        const documentId = `id-${Math.floor(Math.random() * 4294967296)}`;
        const timestamp = new Date();

        await es.indices.create({
            index: indexName,
            body: {
                mappings: {
                    properties: {
                        timestamp: {
                            type: "date",
                            format: "date_time_no_millis",
                        }
                    }
                }
            }
        });

        const resp = await es.index({
            index: indexName,
            id: documentId,
            body: {
                abc: 123,
                timestamp,
            }
        }, {
            ignore: [400],
        });

        expect(resp.statusCode).toStrictEqual(400);
    });
});
