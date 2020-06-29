import { batch } from '../../../import/common/batch';
import moment from "moment";
import {Merge} from "../../../import/common/merge";

describe("batch constructor", () => {
    describe("creates a program that", () => {
        it("performs extract, transform and load, " +
            "on the provided input and output, " +
            "using the provided ETL functions", async () => {
            const input: string[] = ["hello", "world!"];
            const output: Set<{ text: string, length: number }> = new Set();

            const extractor = () => (input: string[]) => async function* () {
                for (const word of input) {
                    yield word;
                }
            };

            const transform = (str: string) => [{
                text: str,
                length: str.length,
            }];

            const loader = async (output: Set<{ text: string, length: number }>) =>
                async (batch: { text: string, length: number }[]) => {
                    for (const record of batch) {
                        output.add(record);
                    }
                };

            const batchJob = batch(extractor, transform, loader)(1, 1);

            await batchJob(input, output);

            expect(output).toStrictEqual(new Set([
                {
                    text: "hello",
                    length: 5,
                },
                {
                    text: "world!",
                    length: 6,
                }
            ]));
        });

        it("parallelizes merged work when parallelism > 1", async () => {
            const input: number[] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            const output: string[] = [];

            const extractor = (merge: Merge) => (input: number[]) => () => merge(
                async function* (i: number) {
                    for (let j = 0; j < 10; j++) {
                        await new Promise(resolve => setTimeout(resolve, 1));
                        yield `${i}.${j}`;
                    }
                }
            )(
                (async function* () {
                    for (const i of input) {
                        await new Promise(resolve => setTimeout(resolve, 5));
                        output.push(`${i}`);
                        yield i;
                    }
                })(),
            );

            const transform = (str: string) => [str];

            const loader = async (output: string[]) =>
                async (batch: string[]) => {
                    for (const id of batch) {
                        output.push(id);
                    }
                    await new Promise(resolve => setTimeout(resolve, 1));
                };

            const batchJob = batch(extractor, transform, loader)(4, 1);

            const before = moment();
            await batchJob(input, output);
            const after = moment();
            expect(after.diff(before)).toBeLessThan(250);
            expect(output).not.toStrictEqual([
                '0', '0.0', '0.1', '0.2', '0.3', '0.4', '0.5', '0.6',
                '0.7', '0.8', '0.9', '1', '1.0', '1.1', '1.2', '1.3',
                '1.4', '1.5', '1.6', '1.7', '1.8', '1.9', '2', '2.0',
                '2.1', '2.2', '2.3', '2.4', '2.5', '2.6', '2.7', '2.8',
                '2.9', '3', '3.0', '3.1', '3.2', '3.3', '3.4', '3.5',
                '3.6', '3.7', '3.8', '3.9', '4', '4.0', '4.1', '4.2',
                '4.3', '4.4', '4.5', '4.6', '4.7', '4.8', '4.9', '5',
                '5.0', '5.1', '5.2', '5.3', '5.4', '5.5', '5.6', '5.7',
                '5.8', '5.9', '6', '6.0', '6.1', '6.2', '6.3', '6.4',
                '6.5', '6.6', '6.7', '6.8', '6.9', '7', '7.0', '7.1',
                '7.2', '7.3', '7.4', '7.5', '7.6', '7.7', '7.8', '7.9',
                '8', '8.0', '8.1', '8.2', '8.3', '8.4', '8.5', '8.6',
                '8.7', '8.8', '8.9', '9', '9.0', '9.1', '9.2', '9.3',
                '9.4', '9.5', '9.6', '9.7', '9.8', '9.9'
            ])
        });

        it("batches work when batchSize > 1", async () => {
            const input: string[] = ["hello", "world!"];
            const output: string[] = [];

            const extractor = () => (input: string[]) => async function* () {
                for (const word of input) {
                    for (const char of word) {
                        yield char;
                    }
                }
            };

            const transform = (str: string) => [str];

            const loader = async (output: string[]) =>
                async (batch: string[]) => {
                    output.push(batch.join(""));
                    await new Promise(resolve => setTimeout(resolve, 1));
                };

            const batchJob = batch(extractor, transform, loader)(1, 3);

            await batchJob(input, output);
            expect(output).toStrictEqual(["hel", "low", "orl", "d!"]);
        });

        it("performs initializing work one time", async () => {
            const input: string[] = ["hello", "world!"];
            const output: { list?: string[] } = {};

            const extractor = () => (input: string[]) => async function* () {
                for (const word of input) {
                    for (const char of word) {
                        yield char;
                    }
                }
            };

            const transform = (str: string) => [str];

            const loader = async (output: { list?: string[] }) => {
                output.list = [];
                return async (batch: string[]) => {
                    output.list!.push(batch.join(""));
                    await new Promise(resolve => setTimeout(resolve, 1));
                };
            };

            const batchJob = batch(extractor, transform, loader)(1, 3);

            await batchJob(input, output);
            expect(output.list).toStrictEqual(["hel", "low", "orl", "d!"]);
        });

        it("merges parallel work into batches", async () => {
            const input: number[] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            const output: string[][] = [];

            const extractor = (merge: Merge) => (input: number[]) => () => merge(
                async function* (i: number) {
                    for (let j = 0; j < 10; j++) {
                        yield `${i}.${j}`;
                    }
                }
            )(
                (async function* () {
                    for (const i of input) {
                        yield i;
                    }
                })(),
            );

            const transform = (str: string) => [str];

            const loader = async (output: string[][]) =>
                async (batch: string[]) => {
                    output.push(batch);
                };

            const batchJob = batch(extractor, transform, loader)(5, 5);

            await batchJob(input, output);
            expect(output).not.toStrictEqual([
                ['0.0', '0.1', '0.2', '0.3', '0.4'],
                ['0.5', '0.6', '0.7', '0.8', '0.9'],
                ['1.0', '1.1', '1.2', '1.3', '1.4'],
                ['1.5', '1.6', '1.7', '1.8', '1.9'],
                ['2.0', '2.1', '2.2', '2.3', '2.4'],
                ['2.5', '2.6', '2.7', '2.8', '2.9'],
                ['3.0', '3.1', '3.2', '3.3', '3.4'],
                ['3.5', '3.6', '3.7', '3.8', '3.9'],
                ['4.0', '4.1', '4.2', '4.3', '4.4'],
                ['4.5', '4.6', '4.7', '4.8', '4.9'],
                ['5.0', '5.1', '5.2', '5.3', '5.4'],
                ['5.5', '5.6', '5.7', '5.8', '5.9'],
                ['6.0', '6.1', '6.2', '6.3', '6.4'],
                ['6.5', '6.6', '6.7', '6.8', '6.9'],
                ['7.0', '7.1', '7.2', '7.3', '7.4'],
                ['7.5', '7.6', '7.7', '7.8', '7.9'],
                ['8.0', '8.1', '8.2', '8.3', '8.4'],
                ['8.5', '8.6', '8.7', '8.8', '8.9'],
                ['9.0', '9.1', '9.2', '9.3', '9.4'],
                ['9.5', '9.6', '9.7', '9.8', '9.9'],
            ]);
            expect(output).not.toContainEqual(['0.0', '0.1', '0.2', '0.3', '0.4']);
        });
    });
});
