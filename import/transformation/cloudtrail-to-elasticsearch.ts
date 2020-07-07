import {CloudtrailLogRecord} from "../types/log/cloudtrail-log";
import {ElasticSearchLogRecord} from "../types/log/elasticsearch-record";


export const convertCloudtrailToElasticsearch = (record: CloudtrailLogRecord): ElasticSearchLogRecord[] => {
    const { userIdentity, requestParameters, responseElements, ...input } = record;
    const output: ElasticSearchLogRecord = {
        ...input,
        raw: JSON.stringify(record),
    };
    if (requestParameters) {
        output.requestParameters = JSON.stringify(requestParameters);
    }
    if (responseElements) {
        output.responseElements = JSON.stringify(responseElements);
    }
    if (userIdentity) {
        const { sessionContext, ...userIdentityOut } = userIdentity;
        if (sessionContext) {
            userIdentityOut.sessionContext = JSON.stringify(sessionContext);
        }
        output.userIdentity = userIdentityOut;
    }
    return [output];
};
