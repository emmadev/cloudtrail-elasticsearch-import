import {LoadParams} from "./params";
import {ElasticSearchLogRecord} from "../types/log/elasticsearch-record";
import debug from "debug";
import {Client as ESClient} from "@elastic/elasticsearch";


/**
 * Ensures the necessary ElasticSearch Indexes Exist
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
        ES.indices.create({
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

export const elasticsearchLogRecordLoader = async (params: LoadParams) => {
    await ensureCloudtrailIndex(params.es, params.program.cloudtrailIndex);
    return async (batch: ElasticSearchLogRecord[]) => {
        const d = debug("elasticsearchLogRecordLoader");
        try {

            const bulk: object[] = batch.flatMap(
                record => [
                    {index: {_index: params.program.cloudtrailIndex}},
                    record,
                ]
            );

            await params.es.bulk({
                body: bulk
            });
        } catch(e) {
            d(`Couldn't import batch: ${e}`);
        }
    };
};
