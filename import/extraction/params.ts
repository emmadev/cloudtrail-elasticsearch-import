import * as S3Client from 'aws-sdk/clients/s3';
import {Client as ESClient} from '@elastic/elasticsearch';

export type ExtractionParams = {
    s3: S3Client,
    es: ESClient,
    workIndex: string,
    bucket: string,
    prefix: string,
}
