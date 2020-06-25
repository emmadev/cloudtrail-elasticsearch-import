import {LoadParams} from "./params";
import {ElasticSearchLogRecord} from "../types/log/elasticsearch-record";
import debug from "debug";

export const elasticsearchLogRecordLoader = (params: LoadParams) => async (batch: ElasticSearchLogRecord[]) => {
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
