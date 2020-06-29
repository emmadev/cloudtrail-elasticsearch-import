export type Merge = <T,U>(f: (t: T) => AsyncIterable<U>) => (ait: AsyncIterable<T>) => AsyncIterable<U>
