export const batch = (parallelism: number, batchSize: number) => <I, CI, O, CO>(
    extractor: (inputClient: CI) => () => AsyncGenerator<I, void>,
    transform: (record: I) => O[],
    loader: (outputClient: CO) => (batch: O[]) => Promise<void>,
) => async (inputClient: CI, outputClient: CO) => {
    const extract = extractor(inputClient);
    const load = loader(outputClient);
    const workers: (() => Promise<void>)[] = [];
    const extracted = extract();
    for (let i = 0; i < parallelism; i++) {
        const transformed: AsyncGenerator<O, void> = (async function* () {
            for await (const input of extracted) {
                yield* transform(input);
            }
        }());
        const batched: AsyncGenerator<O[], void> = (async function* () {
            let batch: O[] = [];
            for await (const output of transformed) {
                batch.push(output);
                if (batch.length === batchSize) {
                    yield batch;
                    batch = [];
                }
            }
            yield batch;
        }());
        workers.push(async () => {
            for await (const batch of batched) {
                await load(batch);
            }
        });
    }
    await Promise.all(workers.map(doWork => doWork()));
};
