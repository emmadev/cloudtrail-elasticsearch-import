import {parallelMerge} from "../../../import/common/parallel-merge";
import moment from "moment";

describe("parallelMerge", () => {
    const takeAll = async <T>(asyncIterable: AsyncIterable<T>): Promise<T[]> => {
        const array: T[] = [];
        for await(const element of asyncIterable) {
            array.push(element);
        }
        return array;
    };

    it("maps each member of an outer AsyncIterable into an inner AsyncIterable, " +
        "merging the inner AsyncIterables in parallel, " +
        "using the specified concurrency", async () => {
        const innerFn = async function* (word: string) {
            for (const c of word) {
                await new Promise(resolve => setTimeout(resolve, 10));
                yield c;
            }
        };

        const outerFn = async function* () {
            yield "Hello,";
            yield "world!";
        };

        const before = moment();
        const result = await takeAll(parallelMerge(2)(innerFn)(outerFn()));
        const duration = moment().diff(before);

        expect(duration).toBeLessThan(120);
        expect(result.join("")).not.toStrictEqual("Hello,world!");
        expect(result.join("")).toMatch(/.*H.*e.*l.*l.*o.*,.*/);
        expect(result.join("")).toMatch(/.*w.*o.*r.*l.*d.*!.*/);
    });

    it("is left-identical", async () => {
        const innerFn = async function*(times: number) {
            for (let i = 0; i < times; i++) {
                yield i;
            }
        };

        const outerFn = async function* () {
            yield 42
        };


        const mergeSingleOuter = parallelMerge(10)(innerFn)(outerFn());
        const simplified = innerFn(42);

        expect(await takeAll(mergeSingleOuter)).toStrictEqual(await takeAll(simplified));
    });

    describe("if yield order doesn't matter", () => {
        it("is right-identical", async () => {
            const innerFn = async function* (value: number) {
                yield value;
            };

            const outerFn = async function* () {
                for (let i = 0; i < 42; i++) {
                    yield i;
                }
            };

            const mergeReturnInner = parallelMerge(10)(innerFn)(outerFn());
            const simplified = outerFn();

            expect((await takeAll(mergeReturnInner)).sort()).toStrictEqual((await takeAll(simplified)).sort());
        });

        it("is associative", async () => {
            const innerFn = async function* (times: number) {
                for (let i = 0; i < times; i++) {
                    yield i;
                }
            };
            const middleFn = async function* (times: number) {
                for (let i = 0; i < times; i += 3) {
                    yield i;
                }
            };
            const outerFn = async function* () {
                for (let i = 0; i < 10; i += 2) {
                    yield i;
                }
            };

            const pMerge = parallelMerge(10);
            const mergeLeft = pMerge(innerFn)(pMerge(middleFn)(outerFn()));
            const mergeRight = pMerge((x: number) => pMerge(innerFn)(middleFn(x)))(outerFn());

            expect((await takeAll(mergeLeft)).sort()).toStrictEqual((await takeAll(mergeRight)).sort());
        })
    });

    describe("if not parallel", () => {
        it("is right-identical", async () => {
            const innerFn = async function* (value: number) {
                yield value;
            };

            const outerFn = async function* () {
                for (let i = 0; i < 42; i++) {
                    yield i;
                }
            };

            const mergeReturnInner = parallelMerge(1)(innerFn)(outerFn());
            const simplified = outerFn();

            expect(await takeAll(mergeReturnInner)).toStrictEqual(await takeAll(simplified));
        });

        it("is associative", async () => {
            const innerFn = async function* (times: number) {
                for (let i = 0; i < times; i++) {
                    yield i;
                }
            };
            const middleFn = async function* (times: number) {
                for (let i = 0; i < times; i += 3) {
                    yield i;
                }
            };
            const outerFn = async function* () {
                for (let i = 0; i < 10; i += 2) {
                    yield i;
                }
            };

            const pMerge = parallelMerge(1);
            const mergeLeft = pMerge(innerFn)(pMerge(middleFn)(outerFn()));
            const mergeRight = pMerge((x: number) => pMerge(innerFn)(middleFn(x)))(outerFn());

            expect(await takeAll(mergeLeft)).toStrictEqual(await takeAll(mergeRight));
        })
    });
});
