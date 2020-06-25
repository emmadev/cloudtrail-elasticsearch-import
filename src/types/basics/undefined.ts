import * as D from "io-ts/lib/Decoder";
import * as E from "io-ts/lib/JsonEncoder";
import * as C from "io-ts/lib/JsonCodec";

export const decoder = D.fromGuard({
    is: (u: unknown): u is undefined => u === undefined,
}, "undefined");

export const encoder: E.JsonEncoder<undefined> = {
    encode: () => null
};

export const codec: C.JsonCodec<undefined> = C.make(decoder, encoder);
