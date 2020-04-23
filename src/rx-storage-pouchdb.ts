import {
    filterInMemoryFields,
    massageSelector
} from 'pouchdb-selector-core';

import { RxStorage, PreparedQuery } from './rx-storate.interface';
import type {
    MangoQuery,
    MangoQuerySortPart,
    PouchDBInstance,
    PouchSettings,
    RxQuery,
    MangoQuerySortDirection
} from './types';
import { CompareFunction } from 'array-push-at-sort-position';
import { flatClone, adapterObject, getHeightOfRevision, nextTick } from './util';
import { SortComparator, QueryMatcher, ChangeEvent } from 'event-reduce-js';
import { runPluginHooks } from './hooks';
import {
    PouchDB
} from './pouch-db';
import { newRxError } from './rx-error';
import { Observable, Subject } from 'rxjs';
import { shareReplay } from 'rxjs/operators';

export class RxStoragePouchDbClass implements RxStorage<PouchDBInstance> {
    public name: string = 'pouchdb';

    private eventStreamCache: WeakMap<PouchDBInstance, {
        stream: Observable<ChangeEvent<any>>;
        primaryKey: string;
    }> = new WeakMap();

    constructor(
        public adapter: any,
        public pouchSettings: PouchSettings = {}
    ) { }

    getSortComparator<RxDocType>(
        primaryKey: string,
        query: MangoQuery<RxDocType>
    ): SortComparator<RxDocType> {
        const sortOptions: MangoQuerySortPart[] = query.sort ? query.sort : [{
            [primaryKey]: 'asc'
        }];
        const massagedSelector = massageSelector(query.selector);
        const inMemoryFields = Object.keys(query.selector);
        const fun: CompareFunction<RxDocType> = (a: RxDocType, b: RxDocType) => {
            // TODO use createFieldSorter
            // TODO make a performance test
            const rows = [a, b].map(doc => {
                // swap primary to _id
                const cloned: any = flatClone(doc);
                const primaryValue = cloned[primaryKey];
                delete cloned[primaryKey];
                cloned._id = primaryValue;
                return {
                    doc: cloned
                };
            });
            const sortedRows: { doc: any }[] = filterInMemoryFields(
                rows,
                {
                    selector: massagedSelector,
                    sort: sortOptions
                },
                inMemoryFields
            );

            if (sortedRows[0].doc._id === rows[0].doc._id) {
                return -1;
            } else {
                return 1;
            }
        };
        return fun;
    }

    /**
     * @link https://github.com/pouchdb/pouchdb/blob/master/packages/node_modules/pouchdb-selector-core/src/matches-selector.js
     */
    getQueryMatcher<RxDocType>(
        primaryKey: string,
        query: MangoQuery<RxDocType>
    ): QueryMatcher<RxDocType> {
        const massagedSelector = massageSelector(query.selector);
        const fun: QueryMatcher<RxDocType> = (doc: RxDocType) => {
            // swap primary to _id
            const cloned: any = flatClone(doc);
            const primaryValue = cloned[primaryKey];
            delete cloned[primaryKey];
            cloned._id = primaryValue;
            const row = {
                doc: cloned
            };
            const rowsMatched = filterInMemoryFields(
                [row],
                { selector: massagedSelector },
                Object.keys(query.selector)
            );
            return rowsMatched && rowsMatched.length === 1;
        };
        return fun;
    }

    createStorageInstance(
        databaseName: string,
        collectionName: string,
        schemaVersion: number,
        options: any = {}
    ): PouchDBInstance {
        if (!options.pouchSettings) {
            options.pouchSettings = {};
        }

        const pouchLocation = getPouchLocation(
            databaseName,
            collectionName,
            schemaVersion
        );
        const pouchDbParameters = {
            location: pouchLocation,
            adapter: adapterObject(this.adapter),
            settings: options.pouchSettings
        };
        const pouchDBOptions = Object.assign(
            {},
            pouchDbParameters.adapter,
            this.pouchSettings,
            pouchDbParameters.settings
        );
        runPluginHooks('preCreatePouchDb', pouchDbParameters);
        return new PouchDB(
            pouchDbParameters.location,
            pouchDBOptions
        ) as any;
    }

    createInternalStorageInstance(
        databaseName: string,
        _options?: any
    ): Promise<PouchDBInstance> {
        const storageInstance = this.createStorageInstance(
            databaseName,
            '_rxdb_internal',
            0,
            {
                pouchSettings: {
                    // no compaction because this only stores local documents
                    auto_compaction: false,
                    revs_limit: 1
                }
            }
        );
        return Promise.resolve(storageInstance);
    }

    /**
     * pouchdb has many bugs and strange behaviors
     * this functions takes a normal mango query
     * and transforms it to one that fits for pouchdb
     */
    prepareQuery<RxDocType>(
        rxQuery: RxQuery<RxDocType>,
        mutateableQuery: MangoQuery<RxDocType>
    ): PreparedQuery<RxDocType> {
        const primPath = rxQuery.collection.schema.primaryPath;
        const query = mutateableQuery;

        /**
         * because sort wont work on unused keys we have to workarround
         * so we add the key to the selector if necessary
         * @link https://github.com/nolanlawson/pouchdb-find/issues/204
         */
        if (query.sort) {
            query.sort.forEach(sortPart => {
                const key = Object.keys(sortPart)[0];
                if (!query.selector[key] || !query.selector[key].$gt) {
                    const schemaObj = rxQuery.collection.schema.getSchemaByObjectPath(key);
                    if (!schemaObj) {
                        throw newRxError('QU5', {
                            key
                        });
                    }
                    if (!query.selector[key]) {
                        query.selector[key] = {};
                    }
                    switch (schemaObj.type) {
                        case 'number':
                        case 'integer':
                            // TODO change back to -Infinity when issue resolved
                            // @link https://github.com/pouchdb/pouchdb/issues/6454
                            // -Infinity does not work since pouchdb 6.2.0
                            query.selector[key].$gt = -9999999999999999999999999999;
                            break;
                        case 'string':
                            /**
                             * strings need an empty string, see
                             * @link https://github.com/pubkey/rxdb/issues/585
                             */
                            if (typeof query.selector[key] !== 'string') {
                                query.selector[key].$gt = '';
                            }
                            break;
                        default:
                            query.selector[key].$gt = null;
                            break;
                    }
                }
            });
        }

        // regex does not work over the primary key
        // TODO move this to dev mode
        if (query.selector[primPath] && query.selector[primPath].$regex) {
            throw newRxError('QU4', {
                path: primPath,
                query: rxQuery.mangoQuery
            });
        }

        // primary-swap sorting
        if (query.sort) {
            const sortArray: MangoQuerySortPart<RxDocType>[] = query.sort.map(part => {
                const key = Object.keys(part)[0];
                const direction: MangoQuerySortDirection = Object.values(part)[0];
                const useKey = key === primPath ? '_id' : key;
                const newPart = { [useKey]: direction };
                return newPart as any;
            });
            query.sort = sortArray;
        }

        // strip empty selectors
        Object.entries(query.selector).forEach(([k, v]) => {
            if (
                typeof v === 'object' &&
                v !== null &&
                !Array.isArray(v) &&
                Object.keys((v as any)).length === 0
            ) {
                delete query.selector[k];
            }
        });

        // primary swap
        if (
            primPath !== '_id' &&
            query.selector[primPath]
        ) {
            // selector
            query.selector._id = query.selector[primPath];
            delete query.selector[primPath];
        }

        // if no selector is used, pouchdb has a bug, so we add a default-selector
        if (Object.keys(query.selector).length === 0) {
            query.selector = {
                _id: {}
            };
        }


        return query;
    }

    getEvents(
        instance: PouchDBInstance,
        primaryKey: string
    ): Observable<ChangeEvent<any>> {
        if (!this.eventStreamCache.has(instance)) {
            const subject: Subject<ChangeEvent<any>> = new Subject();
            const oldBulkDocs = instance.bulkDocs.bind(instance);

            /**
             * overwrite the bulkDocs function
             * like described here
             * @link http://jsbin.com/pagebi/1/edit?js,output
             */
            (instance as any).bulkDocs = async function (
                this: PouchDBInstance,
                body: any[] | { docs: any[], new_edits?: boolean },
                options?: any,
                callback?: Function
            ) {
                // we need to await 2 ticks here,
                // to prevent a race condition when write+read occurs at the same time
                await nextTick();
                await nextTick();

                /*
                console.log('bulk docs input:');
                console.log(JSON.stringify(body, null, 4));
                console.dir(options);
                console.dir(callback);
                console.dir(other);
                console.log('------------------');*/

                // normalize input
                if (typeof options === 'function') {
                    callback = options;
                    options = {};
                }
                let docs: any[];
                if (Array.isArray(body)) {
                    docs = body;
                } else if (body === undefined) {
                    docs = [];
                } else {
                    docs = body.docs;
                }


                // used in internal calls, can update a documents via older revision
                // which does not affect anything
                const noNewEdit = (!!options && options.new_edits === false) || (body as any).new_edits === false;


                if (typeof (body as any).new_edits !== 'undefined') {
                    delete options.new_edits;
                }

                /*
                console.log('bulk docs input normalized:');
                console.log(JSON.stringify(docs, null, 4));
                console.dir(options);
                console.dir(callback);
                console.dir(other);
                console.dir(body);
                console.log('------------------');*/

                // if no new edit is used, we need the previous last winning rev of each document
                // TODO this could be made faster because rxdb already knows some previous values
                // but does not inject them into the call to bulkDocs()
                const previousDocsById = new Map();
                if (!(docs.length === 1 && docs[0]._id.startsWith('_design/'))) { // performance shortcut
                    const previousDocs = await this.allDocs({
                        keys: docs.map(doc => doc._id),
                        include_docs: true
                    });
                    previousDocs.rows.forEach((row: any) => {
                        if (!row.error) {
                            previousDocsById.set(row.key, row.doc);
                        }
                    });
                }

                let result: any[] = [];
                try {
                    result = await new Promise((res, rej) => {
                        (oldBulkDocs as any).call(this, body, options, (err: any, res2: any) => {
                            if (err) {
                                rej(err);
                            } else {
                                res(res2);
                            }
                        });
                    });
                } catch (err) {
                    if (typeof callback === 'function') {
                        callback(err, undefined);
                        return;
                    } else {
                        throw err;
                    }
                }

                if (noNewEdit) {
                    docs.forEach(doc => {
                        const id = doc._id;

                        if (id.startsWith('_design/')) {
                            return;
                        }

                        const prevDoc = previousDocsById.get(id);
                        let event: ChangeEvent<any>;

                        let isNewRevisionHighter = false;
                        if (
                            !prevDoc ||
                            getHeightOfRevision(doc._rev) >
                            getHeightOfRevision(prevDoc._rev)
                        ) {
                            isNewRevisionHighter = true;
                        }

                        if (!isNewRevisionHighter) {
                            return;
                        }

                        if (!prevDoc) {
                            if (doc._deleted) {
                                return;
                            }
                            event = {
                                id,
                                operation: 'INSERT',
                                doc: cleanDoc(doc),
                                previous: null
                            };
                        } else if (
                            doc._deleted
                        ) {
                            event = {
                                id,
                                operation: 'DELETE',
                                doc: null,
                                previous: cleanDoc(doc)
                            };
                        } else {
                            if (prevDoc._deleted) {
                                event = {
                                    id,
                                    operation: 'INSERT',
                                    doc: cleanDoc(doc),
                                    previous: null
                                };
                            } else {
                                event = {
                                    id,
                                    operation: 'UPDATE',
                                    doc: cleanDoc(doc),
                                    previous: cleanDoc(prevDoc)
                                };
                            }
                        }

                        subject.next(event);
                    });
                } else {
                    const inputDocsById = new Map();
                    docs.forEach(doc => inputDocsById.set(doc._id, doc));
                    result.forEach(res => {
                        const id = res.id;

                        if (id.startsWith('_design/')) {
                            return;
                        }

                        if (!res.ok) { return; }
                        const before = previousDocsById.get(id);
                        const after = inputDocsById.get(id);
                        after._rev = res.rev;

                        let event: ChangeEvent<any>;
                        if (after._rev.startsWith('1-')) {
                            if (after._deleted) {
                                return;
                            }
                            event = {
                                id,
                                operation: 'INSERT',
                                doc: cleanDoc(after),
                                previous: null
                            };
                        } else if (after._deleted) {
                            event = {
                                id,
                                operation: 'DELETE',
                                doc: null,
                                previous: cleanDoc(before)
                            };
                        } else {
                            event = {
                                id,
                                operation: 'UPDATE',
                                doc: cleanDoc(after),
                                previous: cleanDoc(before)
                            };
                        }
                        subject.next(event);
                    });
                }

                // console.log('result!');
                // console.dir(result);

                if (typeof callback === 'function') {
                    callback(undefined, result);
                    return;
                } else {
                    return result;
                }
            };

            this.eventStreamCache.set(instance, {
                stream: subject.asObservable().pipe(
                    shareReplay(1)
                ),
                primaryKey
            });
        }
        const cached = this.eventStreamCache.get(instance);
        if ((cached as any).primaryKey !== primaryKey) {
            throw new Error('wrong primary key given');
        }
        return (cached as any).stream;
    }
}

export function cleanDoc<T>(doc: T): T {
    const cloned: any = Object.assign({}, doc);
    delete cloned._revisions;

    if (!cloned._deleted) {
        delete cloned._deleted;
    }

    return cloned;
}

/**
 * returns the pouchdb-database-name
 */
export function getPouchLocation(
    dbName: string,
    collectionName: string,
    schemaVersion: number
): string {
    const prefix = dbName + '-rxdb-' + schemaVersion + '-';
    if (!collectionName.includes('/')) {
        return prefix + collectionName;
    } else {
        // if collectionName is a path, we have to prefix the last part only
        const split = collectionName.split('/');
        const last = split.pop();

        let ret = split.join('/');
        ret += '/' + prefix + last;
        return ret;
    }
}

export function getRxStoragePouchDb(
    adapter: any,
    pouchSettings?: PouchSettings
): RxStorage<PouchDBInstance> {
    if (!adapter) {
        throw new Error('adapter missing');
    }
    return new RxStoragePouchDbClass(adapter, pouchSettings);
}
