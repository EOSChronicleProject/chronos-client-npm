import {ABI, Serializer}  from '@greymass/eosio';

import cassandra from 'cassandra-driver';
import shipABIJSON from '../state_history_plugin_abi.json' assert {type: 'json'};

export class ChronosClient {
    client: cassandra.Client;
    shipABI: ABI;

    constructor(options: cassandra.ClientOptions) {
        this.client = new cassandra.Client(options);

        this.shipABI = new ABI({
            version:  shipABIJSON.version,
            types:    shipABIJSON.types,
            variants: shipABIJSON.variants,
            structs: [],
            actions:  [],
            tables: [],
            ricardian_clauses: [],
        });

        shipABIJSON.structs.forEach((elem) => {
            this.shipABI.structs.push({name: elem.name, base: '', fields: elem.fields});
        });

        shipABIJSON.tables.forEach((elem) => {
            this.shipABI.tables.push({name: elem.name,
                                      index_type: 'i64',
                                      key_names: elem.key_names,
                                      key_types: [],
                                      type: elem.type});

        });
    }


    parseTraceBlob(blob: Buffer): Object {
        let trace = Serializer.decode({abi: this.shipABI, data: blob, type: 'transaction_trace'});
        return trace;
    }

    async getTraceBlobByTrxID(trx_id: string): Promise<Buffer> {
        return new Promise<Buffer>((resolve, reject) => {
            const id_binary = Buffer.from(trx_id, 'hex');
            if( id_binary.length != 32 ) {
                reject(new Error('Transaction ID must be 32 bytes encoded as hex: ' + trx_id));
            }

            this.client.execute('SELECT trace FROM transactions where trx_id = ?',
                                [ id_binary ],
                                { prepare : true })
                .then((result) => {
                    if( result.rows.length > 0 ) {
                        resolve(result.rows[0].trace);
                    }
                    else {
                        reject(new Error('Transaction not found: ' + trx_id));
                    }
                });
        });
    }
}


/*
 Local Variables:
 mode: javascript
 indent-tabs-mode: nil
 End:
*/
