import { createReadStream } from 'fs';

async function execute_request() {
    const stream = createReadStream('./data.json')

    await fetch('https://poc-bi-api.azurewebsites.net/api/ingest?table=tld_bi33_margins_report', {
        method: 'POST',
        body: stream,
        duplex: 'half',
    })
}

execute_request();