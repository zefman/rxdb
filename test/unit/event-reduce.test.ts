import assert from 'assert';
import deepEqual from 'deep-equal';
import { clone } from 'async-test-util';

import * as schemas from '../helper/schemas';
import * as schemaObjects from '../helper/schema-objects';
import {
    createRxDatabase,
    randomCouchString,
    RxCollection,
    RxDocument,
    MangoQuery
} from '../../';

describe('event-reduce.test.js', () => {
    async function createCollection(eventReduce: boolean): Promise<RxCollection> {
        const db = await createRxDatabase({
            name: randomCouchString(10),
            adapter: 'memory',
            eventReduce
        });
        const schema = clone(schemas.primaryHuman);
        schema.keyCompression = false;
        schema.indexes = ['age', 'lastName'];
        const collection = await db.collection({
            name: 'items',
            schema
        });
        return collection;
    }
    function ensureResultsEqual(res1: RxDocument[], res2: RxDocument[]) {
        const ids1 = res1.map(d => d.primary);
        const ids2 = res2.map(d => d.primary);
        if (!deepEqual(ids1, ids2)) {
            console.error('result ids not equal');
            console.dir(ids1);
            console.dir(ids2);
            throw new Error('not equal');
        }

        assert.deepStrictEqual(
            res1.map(d => d.toJSON()),
            res2.map(d => d.toJSON())
        );
    }
    it('should have the same results as without event-reduce', async () => {
        const colNoEventReduce = await createCollection(false);
        const colWithEventReduce = await createCollection(true);


        const queries: MangoQuery[] = [
            {
                selector: {},
                sort: [{
                    passportId: 'asc'
                }]
            },
            {
                selector: {
                    age: {
                        $gt: 20
                    }
                },
                // TODO it should also work without the sorting
                // because RxDB should add predicatble sort if primary not used in sorting
                sort: [{
                    passportId: 'asc'
                }]
            }

        ];

        async function testQueries() {
            await Promise.all(
                queries.map(async (query) => {
                    const res1 = await colNoEventReduce.find(query).exec();
                    const res2 = await colWithEventReduce.find(query).exec();
                    ensureResultsEqual(res1, res2);
                })
            );
        }

        await testQueries();

        // add some
        console.log('::::::::::: add some');
        await Promise.all(
            new Array(5)
                .fill(0)
                .map(async () => {
                    const doc = schemaObjects.human();
                    await colNoEventReduce.insert(doc);
                    await colWithEventReduce.insert(doc);
                })
        );

        await testQueries();

        // update one
        console.log('::::::::::: update one');
        await Promise.all(
            [
                colNoEventReduce,
                colWithEventReduce
            ].map(async (col) => {
                const docToUpdate = await col
                    .findOne()
                    .sort('lastName')
                    .exec(true);
                await docToUpdate.atomicSet('age', 50);
            })
        );

        await testQueries();

        // remove one
        console.log('::::::::::: remove one');
        await Promise.all(
            [
                colNoEventReduce,
                colWithEventReduce
            ].map(async (col) => {
                const doc = await col
                    .findOne()
                    .sort('age')
                    .exec(true);
                await doc.remove();
            })
        );

        await testQueries();

        // remove another one
        console.log('::::::::::: remove another one');
        await Promise.all(
            [
                colNoEventReduce,
                colWithEventReduce
            ].map(async (col) => {
                const doc = await col
                    .findOne()
                    .sort('age')
                    .exec(true);
                await doc.remove();
            })
        );
        await testQueries();

        // clean up
        colNoEventReduce.database.destroy();
        colWithEventReduce.database.destroy();
    });
});
