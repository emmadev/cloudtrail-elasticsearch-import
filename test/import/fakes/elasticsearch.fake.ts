import * as ES from "@elastic/elasticsearch";
import moment, {isMoment, Moment} from "moment";

type FakeIndex = HasFakeMappings & {
    records: {
        [id: string]: Record<string, unknown>
    },
}

type HasFakeMappings = {
    mappings?: {
        properties: {
            [prop: string]: {
                type: "date",
                format?: "date_time_no_millis",
            }
        }
    }
};

// @ts-ignore
export class ESFake implements ES.Client {
    private readonly _indices: {
        [index: string]: FakeIndex
    } = {};

    // @ts-ignore
    async index(params: {
        index: string,
        id: string,
        body: Record<string, unknown>,
    }): Promise<unknown> {
        const index = this._indices[params.index];
        if (!index) {
            throw new Error(`Index ${params.index} not found...`);
        }
        const body = {...params.body};
        for(const prop of Object.keys(body)) {
            const mapping = index.mappings?.properties && index.mappings.properties[prop];
            if (mapping) {
                const value = body[prop];
                if (value) {
                    let date: Moment;
                    if (value instanceof Date) {
                        date = moment(value);
                    } else if (isMoment(value)) {
                        date = value;
                    } else if (typeof value === "string") {
                        date = moment(value);
                    } else {
                        throw new Error(`${value} is not a valid date (property: ${prop})`)
                    }

                    if (mapping.format === "date_time_no_millis") {
                        if (date.milliseconds() != 0) {
                            return { statusCode: 400 }
                        }
                        body[prop] = date.format("YYYY-MM-DDTHH:mm:ssZ")
                    } else {
                        body[prop] = date.format("YYYY-MM-DDTHH:mm:ss.SSSZ")
                    }
                } else {
                    delete body[prop];
                }
            }
        }
        index.records[params.id] = body;
        return {};
    }

    // @ts-ignore
    async get(params: {
        index: string,
        id: string,
    }, options?: {
        ignore?: number[],
    }): Promise<{statusCode: 200, body: { _source: Record<string, unknown> }} | {statusCode: number}> {
        const index = this._indices[params.index];
        if (!index) {
            throw new Error(`Index ${params.index} not found...`);
        }
        const body = index.records[params.id];
        if (body) {
            return {statusCode: 200, body: { _source: body }};
        } else if (options?.ignore?.includes(404)) {
            return {statusCode: 404};
        } else {
            throw new Error(`Record ${params.id} not found in index ${params.index}...`);
        }
    }

    // @ts-ignore
    async bulk(params: {
        body: Record<string, unknown>[]
    }): Promise<unknown> {
        for(let i = 0; i < params.body.length; i+=2) {
            const command = params.body[i];
            const body = params.body[i+1];
            if (command.index) {
                const index = (command.index as {_index: string})._index;
                const id = (body as {id: string}).id;
                this.index({index, id, body});
            }
        }
        return {};
    }

    private async createIndex(params: {
        index: string,
        body?: HasFakeMappings,
    }): Promise<unknown> {
        const index: FakeIndex = {
            records: {},
            mappings: {
                properties: {}
            },
        };
        if (params.body?.mappings) {
            index.mappings = params.body.mappings;
        }
        this._indices[params.index] = this._indices[params.index] || index;
        return {};
    }

    private async indexExists(params: {
        index: string,
    }): Promise<{body: boolean}> {
        return {body: Boolean(this._indices[params.index])};
    }

    // @ts-ignore
    readonly indices = {
        create: this.createIndex.bind(this),
        exists: this.indexExists.bind(this),
    };
}

// @ts-ignore
export const createESFake = (): ES.Client => new ESFake();
