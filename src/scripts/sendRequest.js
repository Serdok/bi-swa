import { createReadStream } from 'fs';

async function execute_request() {
    const stream = createReadStream('./lines.ndjson')

    await fetch('http://127.0.0.1:7071/api/ingest?table=tld_bi33_margins_report', {
        method: 'POST',
        body: stream,
        duplex: 'half',
    })
}

execute_request();