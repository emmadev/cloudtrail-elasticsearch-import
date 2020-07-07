import * as C from "io-ts/lib/JsonCodec";
import * as E from "io-ts/lib/JsonEncoder";
import * as D from "io-ts/lib/Decoder";

export const codec = <X,Y,Z>(
    codec1: C.JsonCodec<X>,
    codec2: C.JsonCodec<Y>,
    codec3: C.JsonCodec<Z>
) => C.intersection(codec1, C.intersection(codec2, codec3));

export const encoder = <X, Y, Z>(
    enc1: E.JsonEncoder<X>,
    enc2: E.JsonEncoder<Y>,
    enc3: E.JsonEncoder<Z>
) => E.intersection(enc1, E.intersection(enc2, enc3));

export const decoder = <X, Y, Z>(
    dec1: D.Decoder<X>,
    dec2: D.Decoder<Y>,
    dec3: D.Decoder<Z>
) => D.intersection(dec1, D.intersection(dec2, dec3));
