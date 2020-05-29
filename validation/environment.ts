import * as t from "io-ts";
import * as Either from "fp-ts/lib/Either";
import {reporter as report} from "io-ts-reporters";
import debug from "debug";

const d = debug("validation:environment");

interface AWSAccessKeyBrand {
    readonly AWSAccessKey: unique symbol
}

interface AWSSecretKeyBrand {
    readonly AWSSecretKey: unique symbol
}

// Ensure key and secret are either both set and in proper format, or both unset.
const EnvV = t.union([
    t.type({
        AWS_ACCESS_KEY: t.brand(
            t.string,
            (s): s is t.Branded<string, AWSAccessKeyBrand> =>
                /(?<![A-Z0-9])[A-Z0-9]{20}(?![A-Z0-9])/.test(s),
            "AWSAccessKey"
        ),
        AWS_SECRET_KEY: t.brand(
            t.string,
            (s): s is t.Branded<string, AWSSecretKeyBrand> =>
                /(?<![A-Za-z0-9/+=])[A-Za-z0-9/+=]{40}(?![A-Za-z0-9/+=])/.test(s),
            "AWSSecretKey"
        ),
    }),
    t.type({
        AWS_ACCESS_KEY: t.undefined,
        AWS_SECRET_KEY: t.undefined,
    }),
]);

export type Env = t.TypeOf<typeof EnvV>;

export const validateEnvironment = (unsafeEnv: unknown): Env => {
    const result = EnvV.decode(unsafeEnv);
    return Either.fold(
        (): Env => {
            d(`FATAL - Unable to validate command line options: ${report(result)}`);
            process.exit(1);
        },
        (v: Env): Env => v,
    )(result);
};

