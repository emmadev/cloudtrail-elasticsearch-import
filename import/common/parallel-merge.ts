import {Merge} from "./merge";

/**
 * Tuple-typed curried pairing operation. Allows for simplifications like:
 * ```["a","b","c"].map(pair(1))
 *     // => [[1,"a"],[1,"b"],[1,"c"]]
 * ```
 * @param a
 */
const pair = <A, B>(a: A) => (b: B): [A, B] => [a, b];

/**
 * Merge implementation that runs the outer loop in a constant number of parallel workers.
 * @param concurrency {number} The number of parallel workers.
 */
export const parallelMerge: (concurrency: number) => Merge =
    concurrency => innerFn => async function*(outer) {
        const createWorker = async function*() {
            for await (const t of outer) {
                for await (const u of innerFn(t)) {
                    yield u;
                }
            }
        };

        const workers = [];
        for(let w = 0; w < concurrency; w++) {
            workers.push(createWorker());
        }

        const promises =
            workers.map((worker, index) => worker.next().then(pair(index)));

        const results: ({done?: boolean} | undefined)[] = workers.map(() => undefined);

        while (results.some(result => !result?.done)) {
            const [index, nextResult] = await Promise.race(
                promises.filter((_, index) => !results[index]?.done)
            );
            results[index] = nextResult;
            if (!nextResult.done) {
                promises[index] = workers[index].next().then(pair(index));
                yield nextResult.value;
            }
        }
    };

