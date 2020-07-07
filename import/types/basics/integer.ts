import * as C from "io-ts/lib/JsonCodec";

export const codec: C.JsonCodec<number> =
    C.refine(
        (num: number): num is number => Number.isInteger(num),
        "integer"
    )(C.number);
