import {LoadParams} from "./params";
import {ElasticSearchLogRecord} from "../types/log/elasticsearch-record";
import debug from "debug";
import {Client as ESClient} from "@elastic/elasticsearch";


/**
 * Ensures the cloudrail index exists
 *
 * @method ensureCloudtrailIndex
 * @param {ESClient} ES initialized Elastical.Client
 * @param {String} cloudtrailIndexName name of index for cloudtrail events
 * @return {Promise<void>}
 */
const ensureCloudtrailIndex = async (ES: ESClient, cloudtrailIndexName: string) => {
    const d = debug("ensureCloudtrailIndex");
    const indices = ES.indices;
    const ctIndex =
        await indices.exists({index: cloudtrailIndexName});

    const makeCTIndex = () =>
        indices.create({
            index: cloudtrailIndexName,
            body: {
                mappings: {
                    properties: {
                        eventTime: {type: "date", format: "date_time_no_millis"}
                    }
                }
            },
        });

    if (ctIndex.body) {
        d(`${cloudtrailIndexName} exists`);
    } else {
        await makeCTIndex();
    }
};

export const elasticsearchLogRecordLoader = async ({es, cloudtrailIndex}: LoadParams) => {
    await ensureCloudtrailIndex(es, cloudtrailIndex);
    return async (batch: ElasticSearchLogRecord[]) => {
        const d = debug("elasticsearchLogRecordLoader");
        try {

            const bulk: object[] = batch.flatMap(
                record => [
                    {index: {_index: cloudtrailIndex}},
                    record,
                ]
            );
            d(`Indexing ${batch.length} records to ${cloudtrailIndex}`);
            await es.bulk({
                body: bulk
            });
        } catch(e) {
            d(`Couldn't import batch: ${e}`);
        }
    };
};

export const _private = {
    ensureCloudtrailIndex
};
