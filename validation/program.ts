import * as t from "io-ts";
import * as Either from "fp-ts/lib/Either";
import debug from "debug";
import {reporter as report} from "io-ts-reporters";

const d = debug("validation:program");

const ProgramV = t.type({
    bucket: t.string,
    region: t.string,
    prefix: t.string,
    elasticsearch: t.string,
    workIndex: t.string,
    cloudtrailIndex: t.string
});

export type Program = Readonly<t.TypeOf<typeof ProgramV>>;

export const validateProgram = (unsafeProgram: unknown): Program => {
    const result = ProgramV.decode(unsafeProgram);
    return Either.fold(
        (): Program => {
            d(`FATAL - Unable to validate command line options: ${report(result)}`);
            process.exit(1);
        },
        (v: Program): Program => v,
    )(result);
};
