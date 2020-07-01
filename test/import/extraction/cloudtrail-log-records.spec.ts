import * as AWS from "aws-sdk";
import {createS3Fake} from "../fakes/s3.fake";
import {_private} from "../../../import/extraction/cloudtrail-log-records";

describe("Cloudtrail Extractor", () => {
    const takeAll = async <T>(asyncIterable: AsyncIterable<T>): Promise<T[]> => {
        const array: T[] = [];
        for await(const element of asyncIterable) {
            array.push(element);
        }
        return array;
    };

    describe("listS3Objs", () => {
        let s3: AWS.S3;
        const bucket = "test-bucket";
        const prefix = "/test-prefix";
        beforeEach(async () => {
            s3 = createS3Fake();
            await s3.createBucket({Bucket: bucket}).promise();
        });
        it("gets all objects at '$bucket/$prefix' and nothing else", async () => {
            for(let i = 0; i < 2001; i++) {
                await s3.putObject({
                    Bucket: bucket,
                    Key: `${prefix}/obj-${i}`,
                    Body: 'a',
                }).promise();
            }
            await s3.putObject({
                Bucket: bucket,
                Key: `/test-prefiks/obj-z`,
                Body: 'a',
            }).promise();
            await s3.createBucket({Bucket: "tezt-bucket"}).promise();
            await s3.putObject({
                Bucket: "tezt-bucket",
                Key: `${prefix}/obj-z`,
                Body: 'a',
            }).promise();

            const objs = await takeAll(_private.listS3Objs(s3, bucket, prefix));

            for (let i = 0; i < 2001; i++) {
                expect(objs.some(obj => obj.Key === `${prefix}/obj-${i}`)).toBeTruthy();
            }
            expect(objs.some(obj => obj.Key === `/test-prefiks/obj-z`)).toBeFalsy();
            expect(objs.some(obj => obj.Key === `${prefix}/obj-z`)).toBeFalsy();
        })
    })
});
