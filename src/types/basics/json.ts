import * as Decoder from "io-ts/lib/Decoder";
import {Json} from "io-ts/lib/JsonEncoder";
import * as Encoder from "io-ts/lib/JsonEncoder";
import * as Codec from "io-ts/lib/JsonCodec";

export const decoder: Decoder.Decoder<Json> =
    Decoder.lazy("Json", () =>
        Decoder.nullable(
            Decoder.union(
                Decoder.string,
                Decoder.number,
                Decoder.boolean,
                Decoder.record(decoder),
                Decoder.array(decoder)
            )
        )
    );

export const encoder: Encoder.JsonEncoder<Json> = Encoder.id;

export const codec: Codec.JsonCodec<Json> = Codec.make(decoder, encoder);
