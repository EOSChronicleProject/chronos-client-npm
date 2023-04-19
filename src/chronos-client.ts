import {ABI, ABIDef}  from '@greymass/eosio/src/chain/abi';
import {abiDecode} from '@greymass/eosio/src/serializer/decoder'

import cassandra from 'cassandra-driver';
import shipABIJSON from '../state_history_plugin_abi.json';

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
        let trace = abiDecode({abi: this.shipABI, data: blob, type: 'transaction_trace'});
        return trace;
    }

    async getTraceBlobByTrxID(trx_id: string): Promise<Buffer> {
        const result = await this.client.execute('SELECT trace FROM transactions where trx_id = ?', [ trx_id ], { prepare : true });
        const row = result.first();
        return row.trace;
    }
}


/*
 Local Variables:
 mode: javascript
 indent-tabs-mode: nil
 End:
*/
