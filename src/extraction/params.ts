import * as S3Client from 'aws-sdk/clients/s3';
import {Client as ESClient} from '@elastic/elasticsearch';
import {Program} from "../types/input/program";

export type ExtractionParams = {
    program: Program,
    s3: S3Client,
    es: ESClient,
}
