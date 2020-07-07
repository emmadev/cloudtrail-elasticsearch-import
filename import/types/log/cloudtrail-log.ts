import * as C from "io-ts/lib/Codec";
import * as json from "../basics/json";

export const CloudtrailLogRecord =
    C.intersect(C.record(json.codec))(
        C.partial({
            userIdentity: C.intersect(C.record(json.codec))(
                C.partial({
                    sessionContext: json.codec,
                }),
            ),
            requestParameters: json.codec,
            responseElements: json.codec,
        }),
    );

export type CloudtrailLogRecord = C.TypeOf<typeof CloudtrailLogRecord>;

export const CloudtrailLog = C.type({
    Records: C.array(CloudtrailLogRecord),
});

export type CloudtrailLog = C.TypeOf<typeof CloudtrailLog>;
