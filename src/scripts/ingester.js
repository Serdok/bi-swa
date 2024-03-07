import { readFile } from 'fs/promises';

async function execute_request() {
    const stream = await readFile('./data.json', {encoding: 'utf-8'})

    await fetch('https://poc-bi-api.azurewebsites.net/api/ingest?table=tld_bi33_margins_report', {
        method: 'POST',
        body: stream,
        duplex: 'half',
    })
}

execute_request();