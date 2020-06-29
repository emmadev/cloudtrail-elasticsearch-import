import {Client as ESClient} from '@elastic/elasticsearch';

export type LoadParams = {
    es: ESClient,
    cloudtrailIndex: string,
}
