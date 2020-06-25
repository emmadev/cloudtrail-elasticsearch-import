import * as C from "io-ts/lib/JsonCodec";

export const codec: C.JsonCodec<number> =
    C.refinement(
        C.number,
        (num): num is number => Number.isInteger(num),
        "integer"
    );
