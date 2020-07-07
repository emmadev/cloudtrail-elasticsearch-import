import * as ES from "@elastic/elasticsearch";
import {createESFake} from "../fakes/elasticsearch.fake";
import {_private, elasticsearchLogRecordLoader} from "../../../import/load/elasticsearch-log-records";
import {ElasticSearchLogRecord} from "../../../import/types/log/elasticsearch-record";

describe("Elasticsearch Loader", () => {
    describe("ensureCloudtrailIndex", () => {
        let es: ES.Client;
        const cloudtrailIndex = "cloudtrail-index";
        beforeEach(async () => {
            es = createESFake();
        });
        it("Creates the cloudtrail index if it does not exist", async () => {
            await _private.ensureCloudtrailIndex(es, cloudtrailIndex);
            expect(await es.indices.exists({index: cloudtrailIndex})).toBeTruthy();
        });
        it("Does not error if cloudtrail index does exist", async () => {
            await es.indices.create({index: cloudtrailIndex});
            await expect(_private.ensureCloudtrailIndex(es, cloudtrailIndex)).resolves.not.toThrow();
        });
    });

    describe("elasticsearchLogRecordLoader", () => {
        let es: ES.Client;
        const cloudtrailIndex = "cloudtrail-index";
        beforeEach(async () => {
            es = createESFake();
        });
        it("Ensures the cloudtrail index exists", async () => {
            await elasticsearchLogRecordLoader({es, cloudtrailIndex});
            expect(await es.indices.exists({index: cloudtrailIndex})).toBeTruthy();
        });
        describe("Creates a program that " +
            "accepts an array of log records and", () => {
            const logRecords: (ElasticSearchLogRecord & {id: string})[] = [];
            for(let i = 0; i < 1000; i++) {
                logRecords.push({
                    id: `${i}`,
                    arbitrary1: "value1",
                    userIdentity: {
                        arbitrary2: "value2",
                        sessionContext: "{\"a\":1}",
                    },
                    requestParameters: "\{\"b\":2}",
                    responseElements: "\{\"c\":3}",
                    raw: "\"raw\"",
                })
            }
            it("imports them into elasticsearch", async () => {
                const loader = await elasticsearchLogRecordLoader({es, cloudtrailIndex});
                await loader(logRecords);
                for(const logRecord of logRecords) {
                    expect((await es.get({ index: cloudtrailIndex, id: logRecord.id })).body._source)
                        .toStrictEqual(logRecords[parseInt(logRecord.id)]);
                }
            })
        });
    })
});
