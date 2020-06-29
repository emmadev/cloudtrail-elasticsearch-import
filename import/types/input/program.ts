import * as D from "io-ts/lib/Decoder";
import * as Either from "fp-ts/lib/Either";
import * as Tree from "fp-ts/lib/Tree";
import {Env} from "./environment";
import debug from "debug";

import * as integer from "../basics/integer";

const d = debug("validation:program");

export const Program = D.intersection(
    Env,
    D.type({
        bucket: D.string,
        region: D.string,
        prefix: D.string,
        elasticsearch: D.string,

        parallelism: integer.codec,
        batchSize: integer.codec,

        workIndex: D.string,
        cloudtrailIndex: D.string,
    }),
);

export type Program = Readonly<D.TypeOf<typeof Program>>;

export const validateProgram = (unsafeProgram: unknown): Program | null => {
    const result = Program.decode(unsafeProgram);
    return Either.getOrElseW(
        (e: D.DecodeError) => {
            d(`FATAL - Unable to validate command line options: ${Tree.drawForest(e)}`);
            return null;
        },
    )(result);
};
