import {Client as ESClient} from '@elastic/elasticsearch';
import {Program} from "../types/input/program";

export type LoadParams = {
    program: Program,
    es: ESClient,
}
