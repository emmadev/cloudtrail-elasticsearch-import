import * as E from "io-ts/lib/JsonEncoder";
import * as C from "io-ts/lib/JsonCodec";
import * as json from "../basics/json";

export const ElasticSearchLogRecord =
    E.intersection(
        E.record(json.encoder),
        E.partial({
            userIdentity: E.intersection(
                E.record(json.encoder),
                E.partial({
                    sessionContext: C.string,
                }),
            ),
            requestParameters: C.string,
            responseElements: C.string,
            raw: C.string,
        }),
    );

export type ElasticSearchLogRecord = E.TypeOf<typeof ElasticSearchLogRecord>;
