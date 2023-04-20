import { Command } from 'commander';
const program = new Command();

import { ChronosClient } from '../chronos-client.js';

const options = program.opts();

program
    .requiredOption('--host [value]', 'ScyllaDB host')
    .requiredOption('--dc [value]', 'ScyllaDB datacenter')
    .requiredOption('--keyspace [value]', 'Chronos keyspace', 'chronos')
    .option('--username [value]', 'DB user')
    .option('--password [value]', 'DB password');

program
    .command('trxid')
    .requiredOption('--id [value]', 'transaction ID')
    .description('retrieve a transaction by ID')
    .action(async (cmdopts) => {

        const client = get_client();
        client.getTraceByTrxID(cmdopts.id).then((result) => {
            console.log(JSON.stringify(result));
            process.exit(0);
        });
    });


program
    .command('acc')
    .requiredOption('--account [value]', 'Account name')
    .requiredOption('--maxrows [number]', 'Maximum rows', '100')
    .description('retrieve latest transactions by account name')
    .action(async (cmdopts) => {

        const client = get_client();
        client.getLastTracesByAccount(cmdopts.account, cmdopts.maxrows).then((result) => {
            console.log(JSON.stringify(result));
            process.exit(0);
        });
    });



program.parse(process.argv);


function get_client() {
    let conn = { contactPoints: [options.host],
                 keyspace: options.keyspace,
                 localDataCenter: options.dc,
                 credentials: {username: '', password: ''} };

    if( options.username != null ) {
        if( options.password == null ) {
            throw Error("password option is required");
        }
        conn.credentials = { username: options.username, password: options.username };
    }

    return new ChronosClient(conn);
}


/*
 Local Variables:
 mode: javascript
 indent-tabs-mode: nil
 End:
*/
