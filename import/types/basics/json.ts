import * as D from "io-ts/lib/Decoder";
import {Json} from "io-ts/lib/JsonEncoder";
import * as E from "io-ts/lib/JsonEncoder";
import * as C from "io-ts/lib/JsonCodec";

export const decoder: D.Decoder<Json> =
    D.lazy("Json", () =>
        D.nullable(
            D.union(
                D.string,
                D.number,
                D.boolean,
                D.record(decoder),
                D.array(decoder)
            )
        )
    );

export const encoder: E.JsonEncoder<Json> = E.id<Json>();

export const codec: C.JsonCodec<Json> = C.make(decoder, encoder);
