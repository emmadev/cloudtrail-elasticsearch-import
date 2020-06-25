export const nestedYield = async function* <T>(nested: AsyncGenerator<AsyncGenerator<T, void>, void>): AsyncGenerator<T, void> {
    for await(const generator of nested) {
        for await(const element of generator) {
            yield element;
        }
    }
};
