import {Merge} from "./merge";
import {parallelMerge} from "./parallel-merge";

export const batch = <I, CI, O, CO>(
    extractor: (merge: Merge) => (inputClient: CI) => () => AsyncIterable<I>,
    transform: (record: I) => O[],
    loader: (outputClient: CO) => Promise<(batch: O[]) => Promise<void>>,
) => (parallelism: number, batchSize: number) =>
    async (inputClient: CI, outputClient: CO) => {
    const extract = extractor(parallelMerge(parallelism))(inputClient);
    const load = await loader(outputClient);
    const workers: (() => Promise<void>)[] = [];
    const extracted = extract();
    for (let i = 0; i < parallelism; i++) {
        const transformed: AsyncIterable<O> = (async function* () {
            for await (const input of extracted) {
                yield* transform(input);
            }
        }());
        const batched: AsyncIterable<O[]> = (async function* () {
            let batch: O[] = [];
            for await (const output of transformed) {
                batch.push(output);
                if (batch.length === batchSize) {
                    yield batch;
                    batch = [];
                }
            }
            if (batch.length > 0) {
                yield batch;
            }
        }());
        workers.push(async () => {
            for await (const batch of batched) {
                await load(batch);
            }
        });
    }
    await Promise.all(workers.map(doWork => doWork()));
};
