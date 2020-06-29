import {getId} from "../../../import/common/get-id";
import fc from "fast-check";

describe("getId", () => {
    it("depends on S3 key", () => {
        expect(getId("bucket","/path/to/key/1"))
            .not.toStrictEqual(getId("bucket", "/path/to/key/2"))
    });

    it("depends on bucket name", () => {
        expect(getId("bucket1", "/path/to/key/1"))
            .not.toStrictEqual(getId("bucket2", "/path/to/key/1"))
    });

    it("is reliably unique to bucket and key", () => {
        fc.assert(fc.property(
            fc.asciiString(), fc.asciiString(),
            fc.asciiString(), fc.asciiString(),
            (bucket1, key1, bucket2, key2) => {
                fc.pre(bucket1 !== bucket2);
                fc.pre(key1 !== key2);
                return getId(bucket1, key1) !== getId(bucket2, key2);
            }))
    });

    it("is constant over bucket and key", () => {
        fc.assert(fc.property(fc.asciiString(), fc.asciiString(),
            (bucket, key) => getId(bucket, key) === getId(bucket, key)
        ));
    });
});
