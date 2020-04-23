/**
 * RxChangeEvents a emitted when something in the database changes
 * they can be grabbed by the observables of database, collection and document
 */

import {
    WriteOperation,
    ChangeEvent as EventReduceChangeEvent,
    ChangeEvent
} from 'event-reduce-js';

import type {
    RxCollection,
    RxDocumentTypeWithRev
} from './types';

import { now } from './util';

export type RxChangeEventJson<DocType = any> = {
    operation: WriteOperation;
    documentId: string;
    documentData: RxDocumentTypeWithRev<DocType> | null;
    previousData?: DocType;
    databaseToken: string;
    collectionName: string;
    isLocal: boolean;
    startTime?: number;
    endTime?: number;
};

export type RxChangeEventBroadcastChannelData = {
    cE: RxChangeEventJson,
    storageToken: string
};

export class RxChangeEvent<DocType = any> {

    constructor(
        public readonly operation: WriteOperation,
        public readonly documentId: string,
        public readonly documentData: RxDocumentTypeWithRev<DocType> | null,
        public readonly databaseToken: string,
        public readonly collectionName: string,
        public readonly isLocal: boolean,
        /**
         * timestam on when the operation was triggered
         * and when it was finished
         * This is optional because we do not have this time
         * for events that come from pouchdbs changestream.
         */
        public startTime?: number,
        public endTime?: number,
        public readonly previousData?: DocType | null
    ) { }

    isIntern(): boolean {
        if (this.collectionName && this.collectionName.charAt(0) === '_') {
            return true;
        } else {
            return false;
        }
    }

    toJSON(): RxChangeEventJson<DocType> {
        const ret: RxChangeEventJson<DocType> = {
            operation: this.operation,
            documentId: this.documentId,
            documentData: this.documentData,
            previousData: this.previousData ? this.previousData : undefined,
            databaseToken: this.databaseToken,
            collectionName: this.collectionName,
            isLocal: this.isLocal,
            startTime: this.startTime,
            endTime: this.endTime
        };
        return ret;
    }

    toEventReduceChangeEvent(): EventReduceChangeEvent<DocType> {
        switch (this.operation) {
            case 'INSERT':
                return {
                    operation: this.operation,
                    id: this.documentId,
                    doc: this.documentData as any,
                    previous: null
                };
            case 'UPDATE':
                return {
                    operation: this.operation,
                    id: this.documentId,
                    doc: this.documentData as any,
                    previous: this.previousData ? this.previousData : 'UNKNOWN'
                };
            case 'DELETE':
                return {
                    operation: this.operation,
                    id: this.documentId,
                    doc: null,
                    previous: this.previousData as DocType
                };
        }
    }
}

export interface RxChangeEventInsert<DocType = any> extends RxChangeEvent<DocType> {
    operation: 'INSERT';
    previousData: null;
}

export interface RxChangeEventUpdate<DocType = any> extends RxChangeEvent<DocType> {
    operation: 'UPDATE';
}

export interface RxChangeEventDelete<DocType = any> extends RxChangeEvent<DocType> {
    operation: 'DELETE';
}

export function changeEventFromStorageStream<DocType>(
    change: ChangeEvent<DocType>,
    collection: RxCollection
) {


    /*
    TODO remove log
console.log('changeEventFromStorageStream()');
console.dir(change);
*/

    const operation = change.operation;

    const doc: RxDocumentTypeWithRev<DocType> = change.doc ? collection._handleFromPouch(change.doc) : null;
    const previous: RxDocumentTypeWithRev<DocType> = change.previous ? collection._handleFromPouch(change.previous) : null;

    const documentId = change.id;

    const ret = new RxChangeEvent<DocType>(
        operation,
        documentId,
        doc,
        collection.database.token,
        collection.name,
        false,
        now(),
        now(),
        previous
    );

    /*
    TODO remove log
console.log('changeEventFromStorageStream(): ret');
console.dir(ret);
*/

    return ret;
}

export function changeEventfromPouchChange<DocType>(
    changeDoc: any,
    collection: RxCollection,
    startTime: number, // time when the event was streamed out of pouchdb
    endTime: number, // time when the event was streamed out of pouchdb
): RxChangeEvent<DocType> {
    let operation: WriteOperation = changeDoc._rev.startsWith('1-') ? 'INSERT' : 'UPDATE';
    if (changeDoc._deleted) {
        operation = 'DELETE';
    }

    // decompress / primarySwap
    let doc: RxDocumentTypeWithRev<DocType> | null = collection._handleFromPouch(changeDoc);
    const documentId: string = (doc as any)[collection.schema.primaryPath] as string;

    let previous = null;
    if (operation === 'DELETE') {
        previous = doc;
        doc = null;
    }

    const cE = new RxChangeEvent<DocType>(
        operation,
        documentId,
        doc,
        collection.database.token,
        collection.name,
        false,
        startTime,
        endTime,
        previous
    );
    return cE;
}


export function createInsertEvent<RxDocumentType>(
    collection: RxCollection<RxDocumentType>,
    docData: RxDocumentTypeWithRev<RxDocumentType>,
    startTime: number,
    endTime: number
): RxChangeEvent<RxDocumentType> {
    const ret = new RxChangeEvent<RxDocumentType>(
        'INSERT',
        (docData as any)[collection.schema.primaryPath],
        docData,
        collection.database.token,
        collection.name,
        false,
        startTime,
        endTime,
        null
    );
    return ret;

}

export function createUpdateEvent<RxDocumentType>(
    collection: RxCollection<RxDocumentType>,
    docData: RxDocumentTypeWithRev<RxDocumentType>,
    previous: RxDocumentType,
    startTime: number,
    endTime: number
): RxChangeEvent<RxDocumentType> {
    return new RxChangeEvent<RxDocumentType>(
        'UPDATE',
        (docData as any)[collection.schema.primaryPath],
        docData,
        collection.database.token,
        collection.name,
        false,
        startTime,
        endTime,
        previous
    );
}

export function createDeleteEvent<RxDocumentType>(
    collection: RxCollection<RxDocumentType>,
    docData: RxDocumentTypeWithRev<RxDocumentType>,
    previous: RxDocumentType,
    startTime: number,
    endTime: number,
): RxChangeEvent<RxDocumentType> {
    return new RxChangeEvent<RxDocumentType>(
        'DELETE',
        (docData as any)[collection.schema.primaryPath],
        docData,
        collection.database.token,
        collection.name,
        false,
        startTime,
        endTime,
        previous
    );
}

export function isInstanceOf(obj: RxChangeEvent<any> | any): boolean {
    return obj instanceof RxChangeEvent;
}
