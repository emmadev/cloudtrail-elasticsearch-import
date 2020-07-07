import * as C from "io-ts/lib/JsonCodec";
import * as E from "io-ts/lib/JsonEncoder";
import * as D from "io-ts/lib/Decoder";

export const codec = <X,Y,Z>(
    codec1: C.JsonCodec<X>,
    codec2: C.JsonCodec<Y>,
    codec3: C.JsonCodec<Z>
) => C.intersect(codec1)(C.intersect(codec2)(codec3));

export const encoder = <X, Y, Z>(
    enc1: E.JsonEncoder<X>,
    enc2: E.JsonEncoder<Y>,
    enc3: E.JsonEncoder<Z>
) => E.intersect(enc1)(E.intersect(enc2)(enc3));

export const decoder = <X, Y, Z>(
    dec1: D.Decoder<X>,
    dec2: D.Decoder<Y>,
    dec3: D.Decoder<Z>
) => D.intersect(dec1)(D.intersect(dec2)(dec3));
