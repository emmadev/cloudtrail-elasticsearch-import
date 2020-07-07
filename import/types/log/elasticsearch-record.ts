import * as E from "io-ts/lib/JsonEncoder";
import * as C from "io-ts/lib/JsonCodec";
import * as json from "../basics/json";
import * as intersection3 from "../basics/intersection3";

export const ElasticSearchLogRecord =
    intersection3.encoder(
        E.record(json.encoder),
        E.partial({
            userIdentity: E.intersect(E.record(json.encoder))(
                E.partial({
                    sessionContext: C.string,
                }),
            ),
            requestParameters: C.string,
            responseElements: C.string,
        }),
        E.type({
            raw: C.string,
        })
    );

export type ElasticSearchLogRecord = E.TypeOf<typeof ElasticSearchLogRecord>;
