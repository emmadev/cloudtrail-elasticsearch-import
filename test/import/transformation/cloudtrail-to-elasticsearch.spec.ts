import fc from "fast-check";
import {CloudtrailLogRecord} from "../../../import/types/log/cloudtrail-log";
import {convertCloudtrailToElasticsearch} from "../../../import/transformation/cloudtrail-to-elasticsearch";

describe("Cloudtrail to Elasticsearch transformer", () => {
    it("Maps the required properties into the appropriate format", () => {
        fc.check(fc.property(
            fc.object(), // properties
            fc.option(fc.object()), // userIdentity properties
            fc.record({
                userIdentity: fc.option(fc.record({
                    sessionContext: fc.option(fc.jsonObject()),
                })),
                requestParameters: fc.jsonObject(),
                responseElements: fc.jsonObject(),
            }),
            (
                props,
                userIdentityProps,
                definedProps,
            ) => {
                fc.pre(Boolean(userIdentityProps) === Boolean(definedProps.userIdentity));
                const record =
                    (userIdentityProps
                        ? {
                            ...props,
                            ...definedProps,
                            userIdentity: {...userIdentityProps, ...definedProps.userIdentity},
                        }
                        : {
                            ...props,
                            ...definedProps,
                        }) as CloudtrailLogRecord;

                const transformed = convertCloudtrailToElasticsearch(record);
                expect(transformed).toHaveLength(1);
                for(const [key, value] of Object.entries(props)) {
                    if (!["raw", "userIdentity", "requestParameters", "responseElements"].includes(key)) {
                        expect(transformed[0][key]).toStrictEqual(value);
                    }
                }
                if (definedProps.requestParameters !== null) {
                    expect(transformed[0].requestParameters).toStrictEqual(JSON.stringify(definedProps.requestParameters));
                } else {
                    expect(transformed[0]).not.toHaveProperty("requestParameters")
                }
                if (definedProps.responseElements !== null) {
                    expect(transformed[0].responseElements).toStrictEqual(JSON.stringify(definedProps.responseElements));
                } else {
                    expect(transformed[0]).not.toHaveProperty("responseElements")
                }
                expect(transformed[0].raw).toStrictEqual(JSON.stringify(record));
                if (definedProps.userIdentity !== null) {
                    expect(transformed[0]).toHaveProperty("userIdentity");
                    for (const [key, value] of Object.entries(userIdentityProps as object)) {
                        if (key !== "sessionContext") {
                            expect(transformed[0][key]).toStrictEqual(value);
                        }
                    }
                    if(definedProps.userIdentity.sessionContext) {
                        expect(transformed[0].userIdentity?.sessionContext).toStrictEqual(JSON.stringify(definedProps.userIdentity.sessionContext))
                    } else {
                        expect(transformed[0].userIdentity).not.toHaveProperty("sessionContext");
                    }
                } else {
                    expect(transformed[0]).not.toHaveProperty("userIdentity")
                }
                return true;
            }
        ))
    })
});
