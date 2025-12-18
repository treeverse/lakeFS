import { describe, test, expect } from "vitest";

import { iterateAPI, Pagination, Paginator } from './apiStream';

// Return a paginated API to return list.
function paginateList(list: readonly [string]): (pagination: readonly Pagination) => Promise<Paginator<string>> {
    return async (pagination: readonly Pagination) => {
        let filtered = list;
        let nextOffset: string;
        let hasMore = false;
        if (pagination.prefix) {
            filtered = filtered.filter(x => x.startsWith(pagination.prefix));
        }
        if (pagination.after) {
            filtered = filtered.filter(x => x > pagination.after)
        }
        if (pagination.amount) {
            if (filtered.length > pagination.amount) {
                nextOffset = filtered[pagination.amount-1];
                filtered = filtered.slice(0, pagination.amount);
                hasMore = true;
            }
        }
        return Promise.resolve({
            next_offset: nextOffset, has_more: hasMore, results: filtered,
        });
    };
}

// Return a list of num strings, sorted lexicographically.
function makeStrings(num: number): [string] {
    const ret: [string] = [];
    for (let i = 0; i < num; i++) {
        let s = `000000000000000${i}`;
        s = s.substr(s.length - 15);
        ret.push(`${s}`);
    }
    return ret;
}

describe("iterateAPI", async () => {
    describe.each([
        undefined,
        3,
        7,
    ])('amount %i', (amount) => {
        test("single call to API", async() => {
            const list = makeStrings(300);

            const result: [string] = [];
            const iterator = iterateAPI({amount}, paginateList(list));
            for await (const s of iterator) {
                result.push(s);
            }

            expect(result).toEqual(list);
        });

        test("single call to API with after", async() => {
            const list = makeStrings(300);
            const offset = 123;

            const result: [string] = [];
            const iterator = iterateAPI({after: list[offset], amount}, paginateList(list));
            for await (const s of iterator) {
                result.push(s);
            }

            expect(result).toEqual(list.slice(offset+1));
        });

        test("single call to API with prefix", async() => {
            const list1 = makeStrings(10);
            const list2 = makeStrings(10).map(x => `m-${x}`);
            const list3 = makeStrings(12).map(x => `z-${x}`);
            const list = [].concat(list1, list2, list3);

            const result: [string] = [];
            const iterator = iterateAPI({prefix: 'm-', amount}, paginateList(list));
            for await (const s of iterator) {
                result.push(s);
            }

            expect(result).toEqual(list2);
        });

        test("multiple calls to API", async() => {
            const list = makeStrings(11);

            const result: [string] = [];
            const iterator = iterateAPI({amount}, paginateList(list));
            for await (const s of iterator) {
                result.push(s);
            }

            expect(result).toEqual(list);
        });

        test("break into multiple for loops", async() => {
            // Not really a test of iterateAPI, more a demonstration of how it "freezes"
            // execution.
            const list = makeStrings(300);
            const breakAfter = 123;
            
            const iterator = iterateAPI({amount}, paginateList(list));
            const result1: [string] = [];
            // `for await (... of iterator)` restart the iterator.
            let i: number;
            for(i = 0; i < breakAfter; i++) {
                result1.push((await iterator.next()).value);
            }
            const result2: [string] = [];
            for (;;) {
                const res = await iterator.next();
                if (res.done) {
                    break;
                }
                result2.push(res.value);
            }

            expect(result1.length).toEqual(breakAfter);
            expect(result2.length).toEqual(300 - breakAfter);
            expect([].concat(result1, result2)).toEqual(list);
        });
    })
});
