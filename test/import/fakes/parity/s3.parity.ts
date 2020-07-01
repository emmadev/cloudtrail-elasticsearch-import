import * as AWS from "aws-sdk";
import {createS3Fake} from "../s3.fake";

describe("S3 Fake", () => {

    const parity = <T>(name: string, fn: (s3: AWS.S3) => T, timeout?: number) => {
        const realS3 = new AWS.S3({region: "us-east-1"});
        const fakeS3 = createS3Fake();

        describe(name, () => {
            beforeEach(() => jest.setTimeout(15_000));
            it("Test Fake", () => fn(fakeS3), timeout);
            it("Real Client", () => fn(realS3), timeout);
            afterEach(() => jest.setTimeout(5_000));
        })
    };

    parity("Matches behavior for CreateBucket, PutObject, ListObjects, and GetObject", async s3 => {
        const bucketName = `testing-bucket-${Math.floor(Math.random() * 4294967296)}`;
        await s3.createBucket({Bucket: bucketName}).promise();
        await s3.putObject({
            Bucket: bucketName,
            Key: "/test/test-object",
            Body: "This is test content.",
        }).promise();
        await s3.putObject({
            Bucket: bucketName,
            Key: "/zest/test-object",
            Body: "I don't need this content.",
        }).promise();
        const listResponse = await s3.listObjectsV2({
            Bucket: bucketName,
            Prefix: "/test",
        }).promise();
        expect(listResponse.Contents!.some(object => object.Key === "/test/test-object")).toBeTruthy();
        expect(listResponse.Contents!.some(object => object.Key === "/zest/test-object")).toBeFalsy();
        const stream = s3.getObject({
            Bucket: bucketName,
            Key: "/test/test-object",
        }).createReadStream();
        let content = "";
        for await(const data of stream) {
            content += data.toString("utf8");
        }
        expect(content).toStrictEqual("This is test content.");
    });

    parity("ListObjects returns multiple if multiple match prefix", async s3 => {
        const bucketName = `testing-bucket-${Math.floor(Math.random() * 4294967296)}`;
        await s3.createBucket({Bucket: bucketName}).promise();
        await s3.putObject({
            Bucket: bucketName,
            Key: "/test/test-object-1",
            Body: "This is test content.",
        }).promise();
        await s3.putObject({
            Bucket: bucketName,
            Key: "/test/test-object-2",
            Body: "This is test content.",
        }).promise();
        await s3.putObject({
            Bucket: bucketName,
            Key: "/test/test-object-3",
            Body: "This is test content.",
        }).promise();
        await s3.putObject({
            Bucket: bucketName,
            Key: "/test/test-object-4",
            Body: "This is test content.",
        }).promise();
        const listResponse = await s3.listObjectsV2({
            Bucket: bucketName,
            Prefix: "/test",
        }).promise();
        expect(listResponse.Contents!.some(object => object.Key === "/test/test-object-1")).toBeTruthy();
        expect(listResponse.Contents!.some(object => object.Key === "/test/test-object-2")).toBeTruthy();
        expect(listResponse.Contents!.some(object => object.Key === "/test/test-object-3")).toBeTruthy();
    });

    parity("ListObjects returns a marker if MaxKeys is exceeded.", async s3 => {
        const bucketName = `testing-bucket-${Math.floor(Math.random() * 4294967296)}`;
        await s3.createBucket({Bucket: bucketName}).promise();
        const promises: Promise<unknown>[] = [];
        for(let i = 0; i < 2001; i++) {
            promises.push(s3.putObject({
                Bucket: bucketName,
                Key: `/test/test-object-${i+1}`,
                Body: "This is test content.",
            }).promise());
        }
        await Promise.all(promises);

        const allContents: AWS.S3.Object[] = [];
        let listResponse = await s3.listObjectsV2({
            Bucket: bucketName,
            Prefix: "/test",
        }).promise();

        allContents.push(...listResponse.Contents!);
        let nextMarker = listResponse.NextContinuationToken;
        expect(nextMarker).toBeTruthy();

        listResponse = await s3.listObjectsV2({
            Bucket: bucketName,
            Prefix: "/test",
            ContinuationToken: nextMarker,
        }).promise();

        allContents.push(...listResponse.Contents!);
        nextMarker = listResponse.NextContinuationToken;
        expect(nextMarker).toBeTruthy();

        listResponse = await s3.listObjectsV2({
            Bucket: bucketName,
            Prefix: "/test",
            ContinuationToken: nextMarker,
        }).promise();

        allContents.push(...listResponse.Contents!);
        nextMarker = listResponse.NextContinuationToken;
        expect(nextMarker).toBeFalsy();

        for (let i = 0; i < 2001; i++) {
            expect(allContents.some(object => object.Key === `/test/test-object-${i+1}`)).toBeTruthy();
        }
    });
});
