import {ABI, Serializer, ABISerializableObject, ABIDecoder}  from '@greymass/eosio';

import cassandra from 'cassandra-driver';
import Long from 'long';

import shipABIJSON from '../state_history_plugin_abi.json' assert {type: 'json'};



class StupidVariant implements ABISerializableObject {
    static abiName = '____stupid';
    obj: {};

    constructor(value: {}) {
        this.obj = value;
    }

    static from(value: [string, {}]): TransactionTrace {
        return new this(value[1]);
    }

    toJSON() {
        return this.obj;
    }

    equals(other: StupidVariant) : boolean {
        return true;
    }
};


class ActionReceipt extends StupidVariant {
    static abiName = 'action_receipt';
};

class ActionTrace extends StupidVariant {
    static abiName = 'action_trace';
};

class PartialTransaction extends StupidVariant {
    static abiName = 'partial_transaction';
};

class TransactionTrace extends StupidVariant {
    static abiName = 'transaction_trace';
};


export class TransactionDetails {
    block_num: number;
    block_time: Date;
    trace: {};
    constructor( block_num: number, block_time: Date, trace: {}) {
        this.block_num = block_num;
        this.block_time = block_time;
        this.trace = trace;
    }
};


export class ChronosClient {
    client: cassandra.Client;
    shipABI: ABI;
    abi_cache: Map<string, ABI> = new Map<string, ABI>();

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


    async parseTraceBlob( block_num: number, block_time: Date, blob: Buffer): Promise<TransactionDetails> {
        let trace = Serializer.decode({abi: this.shipABI, data: blob, type: 'transaction_trace',
                                       customTypes:
                                       [
                                           ActionReceipt,
                                           ActionTrace,
                                           PartialTransaction,
                                           TransactionTrace,
                                       ]
                                      });

        let contracts = new Set<string>();
        trace['obj']['action_traces'].forEach((action) => {
            contracts.add(action['obj']['act']['account'].toString());
        });

        let promises = new Array();

        for( const contract of contracts ) {
            if( !this.abi_cache.has(contract) ) {
                promises.push(
                    this.getABI(contract, block_num)
                        .then((abi) => {
                            this.abi_cache.set(contract, abi);
                        }));
            }
        }

        await Promise.all(promises);

        trace['obj']['action_traces'].forEach((action) => {
            const act = action['obj']['act'];
            let decoded_data;
            try {
                decoded_data = Serializer.decode({abi: this.abi_cache.get(act.account.toString()),
                                                  data: act.data,
                                                  type: act.name.toString()});
            }
            catch(err) {
                console.error(err);
            }

            if( decoded_data ) {
                act.data = decoded_data;
            }
        });

        return new TransactionDetails(block_num, block_time, trace);
    }

    async getABI(account: string, block_num: number): Promise<ABI>{
        return new Promise<ABI>((resolve, reject) => {
            this.client.execute('SELECT abi_raw FROM abi_by_account where account_name = ? AND block_num <= ? ' +
                                'ORDER BY block_num DESC LIMIT 1',
                                [ account, block_num ],
                                { prepare : true })
                .then((result) => {
                    if( result.rows.length > 0 ) {
                        const decoder = new ABIDecoder(result.rows[0].abi_raw);
                        resolve(ABI.fromABI(decoder));
                    }
                    else {
                        resolve(new ABI({}));
                    }
                });
        });
    }


    async getTraceByTrxID(trx_id: string): Promise<TransactionDetails> {
        return new Promise<TransactionDetails>((resolve, reject) => {
            const id_binary = Buffer.from(trx_id, 'hex');
            if( id_binary.length != 32 ) {
                reject(new Error('Transaction ID must be 32 bytes encoded as hex: ' + trx_id));
            }

            this.client.execute('SELECT block_num, block_time, trace FROM transactions where trx_id = ?',
                                [ id_binary ],
                                { prepare : true })
                .then((result) => {
                    if( result.rows.length > 0 ) {
                        const row = result.rows[0];
                        resolve(this.parseTraceBlob(row.block_num, row.block_time, row.trace));
                    }
                    else {
                        reject(new Error('Transaction not found: ' + trx_id));
                    }
                });
        });
    }


    async getTracesBySeq(seqs: Array<Long>): Promise<Array<TransactionDetails>> {
        let traces: Map<Long, TransactionDetails> = new Map<Long, TransactionDetails>();

        while( seqs.length > 0 ) {
            let batch: Array<Long> = seqs.splice(0, 100);
            let promises = new Array();
            for( const seq of batch ) {
                promises.push(
                    this.client.execute('SELECT block_num, block_time, trace FROM transactions where seq = ?',
                                        [ seq ],
                                        { prepare : true })
                        .then((result) => {
                            if( result.rows.length > 0 ) {
                                const row = result.rows[0];
                                return this.parseTraceBlob(row.block_num, row.block_time, row.trace)
                                    .then((trdet) => {
                                        traces.set(seq, trdet);
                                    })
                                    .catch((err) => { return Promise.reject(err) });
                            }
                            else {
                                return Promise.reject(new Error('Transaction not found for seq=' + seq));
                            }
                        }));
            }

            await Promise.all(promises);
        }

        let ret: Array<TransactionDetails> = new Array<TransactionDetails>();

        [...traces.entries()].sort((a, b) => a[0].compare(b[0])).forEach((entry) => {
            ret.push(entry[1]);
        });

        return ret;
    }

    async getLastTracesByAccount(account: string, maxcount: number): Promise<Array<TransactionDetails>> {
        let seqs: Array<Long> = new Array<Long>();
        let finished = false;
        let curr_block_date = Long.MAX_VALUE;
        while( seqs.length < maxcount && !finished ) {
            const dates_result =
                  await this.client.execute('SELECT block_date FROM receipt_dates WHERE account_name = ? and block_date < ? ' +
                                            ' ORDER BY block_date DESC LIMIT 10',
                                            [ account, curr_block_date ],
                                            { prepare : true });
            if( dates_result.rowLength == 0 ) {
                finished = true;
            }
            else {
                for( const dates_row of dates_result.rows ) {
                    curr_block_date = dates_row.block_date;

                    const seqs_result =
                          await this.client.execute('SELECT seq FROM account_receipts WHERE account_name = ? and block_date = ? ' +
                                                    'ORDER BY recv_sequence_start DESC LIMIT ?',
                                                    [ account, curr_block_date, maxcount - seqs.length ],
                                                    { prepare : true });
                    for await (const seqs_row of seqs_result) {
                        seqs.push(seqs_row.seq);
                    }

                    if( seqs.length >= maxcount ) {
                        finished = true;
                        break;
                    }
                }
            }
        }
        return this.getTracesBySeq(seqs);
    }
}


/*
 Local Variables:
 mode: javascript
 indent-tabs-mode: nil
 End:
*/
