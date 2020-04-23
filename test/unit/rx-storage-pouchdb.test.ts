import assert from 'assert';
import AsyncTestUtil from 'async-test-util';

import config from './config';
import * as humansCollection from './../helper/humans-collection';
import * as schemaObjects from '../helper/schema-objects';
import {
    getRxStoragePouchDb,
    RxStorage,
    randomCouchString,
    PouchDBInstance
} from '../../';


config.parallel('rx-storage-pouchdb.test.js', () => {
    describe('.getSortComparator()', () => {
        it('should sort in the correct order', async () => {
            const col = await humansCollection.create(1);
            const storage: RxStorage = getRxStoragePouchDb('memory');

            const query = col
                .find()
                .limit(1000)
                .sort('age')
                .toJSON();
            const comparator = storage.getSortComparator(
                col.schema.primaryPath,
                query
            );
            const doc1: any = schemaObjects.human();
            doc1._id = 'aa';
            doc1.age = 1;
            const doc2: any = schemaObjects.human();
            doc2._id = 'bb';
            doc2.age = 100;

            // should sort in the correct order
            assert.deepStrictEqual(
                [doc1, doc2],
                [doc1, doc2].sort(comparator)
            );
            col.database.destroy();
        });
    });
    describe('.getQueryMatcher()', () => {
        it('should match the right docs', async () => {
            const col = await humansCollection.create(1);

            const storage: RxStorage = getRxStoragePouchDb('memory');
            const queryMatcher = storage.getQueryMatcher(
                col.schema.primaryPath,
                col.find({
                    selector: {
                        age: {
                            $gt: 10,
                            $ne: 50
                        }
                    }
                }).toJSON()
            );

            const doc1: any = schemaObjects.human();
            doc1._id = 'aa';
            doc1.age = 1;
            const doc2: any = schemaObjects.human();
            doc2._id = 'bb';
            doc2.age = 100;

            assert.strictEqual(queryMatcher(doc1), false);
            assert.strictEqual(queryMatcher(doc2), true);

            col.database.destroy();
        });
    });

    describe('.getEvents()', () => {
        it('should stream a single write', async () => {
            return;
            const storage: RxStorage = getRxStoragePouchDb('memory');
            const instance: PouchDBInstance = storage.createStorageInstance(
                randomCouchString(),
                'docs',
                0,
                {}
            );

            const events$ = storage.getEvents(
                instance,
                '_id'
            );
            const emitted: any[] = [];
            const sub = events$.subscribe(ev => emitted.push(ev));

            await instance.put({
                _id: 'foo',
                name: 'bar'
            });

            assert.strictEqual(emitted[0].operation, 'INSERT');

            sub.unsubscribe();
        });
        it('should stream an update and delete', async () => {
            return;
            const storage: RxStorage = getRxStoragePouchDb('memory');
            const instance: PouchDBInstance = storage.createStorageInstance(
                randomCouchString(),
                'docs',
                0,
                {}
            );
            const events$ = storage.getEvents(
                instance,
                '_id'
            );
            const emitted: any[] = [];
            const sub = events$.subscribe(ev => emitted.push(ev));

            const putRes = await instance.put({
                _id: 'foo',
                name: 'bar'
            });
            const updateRes = await instance.put({
                _id: 'foo',
                name: 'bar2',
                _rev: putRes.rev
            });
            await instance.remove({
                _id: 'foo',
                name: 'bar3',
                _rev: updateRes.rev
            });

            assert.strictEqual(emitted.length, 3);
            assert.strictEqual(emitted[1].operation, 'UPDATE');
            assert.strictEqual(emitted[2].operation, 'DELETE');

            sub.unsubscribe();
        });
        it('should emit event when write comes from replication', async () => {
            return;
            const storage: RxStorage = getRxStoragePouchDb('memory');
            const instance: PouchDBInstance = storage.createStorageInstance(
                randomCouchString(),
                'docs',
                0,
                {}
            );
            const instance2: PouchDBInstance = storage.createStorageInstance(
                randomCouchString(),
                'docs',
                0,
                {}
            );

            const events$ = storage.getEvents(
                instance,
                '_id'
            );
            const emitted: any[] = [];
            const sub = events$.subscribe(ev => emitted.push(ev));

            const syncState = instance.sync(instance2, {
                live: true
            });

            await instance2.put({
                _id: 'foo',
                name: 'bar'
            });

            await AsyncTestUtil.waitUntil(() => emitted.length === 1);

            assert.strictEqual(emitted[0].operation, 'INSERT');

            sub.unsubscribe();
            syncState.cancel();
        });
        it('should emit all events from replication', async () => {
            return;

            await AsyncTestUtil.wait(1000);
            console.log('###################################');
            console.log('###################################');
            console.log('###################################');
            console.log('###################################');
            console.log('###################################');
            console.log('###################################');
            console.log('###################################');

            const storage: RxStorage = getRxStoragePouchDb('memory');
            const instance: PouchDBInstance = storage.createStorageInstance(
                randomCouchString(),
                'docs',
                0,
                {}
            );
            const instance2: PouchDBInstance = storage.createStorageInstance(
                randomCouchString(),
                'docs',
                0,
                {}
            );

            const events$ = storage.getEvents(
                instance,
                '_id'
            );
            const emitted: any[] = [];
            const sub = events$.subscribe(ev => emitted.push(ev));


            const syncState = instance.sync(instance2, {
                live: true
            });

            const putRes = await instance2.put({
                _id: 'foo',
                name: 'bar'
            });
            await AsyncTestUtil.waitUntil(() => emitted.length === 1);


            console.log('putRes:');
            console.dir(putRes);

            await instance2.put({
                _id: 'foo',
                name: 'bar2',
                _rev: putRes.rev
            });
            await AsyncTestUtil.waitUntil(() => emitted.length === 2);

            /*
                        await instance2.remove({
                            _id: 'foo',
                            name: 'bar3',
                            _rev: updateRes.rev
                        });
                        await AsyncTestUtil.waitUntil(() => emitted.length === 3);

                        assert.strictEqual(emitted[0].operation, 'INSERT');
                        assert.strictEqual(emitted[1].operation, 'UPDATE');
                        assert.strictEqual(emitted[2].operation, 'DELETE');
            */

            console.log('emitted:');
            console.dir(emitted);

            process.exit();

            sub.unsubscribe();
            syncState.cancel();
        });

    });

});
