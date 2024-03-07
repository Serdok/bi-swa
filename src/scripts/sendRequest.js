import { readFile, writeFile } from "fs/promises";
import { gzipSync } from "zlib";

async function execute_request() {
  try {
    const data = await readFile("./dau_item.json", { encoding: "utf-8", });

    const json = JSON.parse(data);
    console.log(`Read file: ${json.length} rows`);

    const chunk_size = 10000;
    const requests = [];
    for (let i = 0; i < json.length; i += chunk_size) {
      const chunk = JSON.stringify(json.slice(i, i + chunk_size));

      const compressed_chunk = gzipSync(chunk);

      console.log(`compressed ${new TextEncoder().encode(chunk).byteLength} bytes down to ${compressed_chunk.byteLength} bytes`);

      const req = fetch("https://poc-bi-api.azurewebsites.net/api/enqueue?table=dau_item", {
          // fetch('http://127.0.0.1:7071/api/enqueue?table=tld_bi33_margins_report', {
          method: "POST",
          body: JSON.stringify(chunk),
          headers: { "Content-Type": "application/json", "Content-Encoding": "gzip", },
        }
      );

      requests.push(req);
    }

    await Promise.allSettled(requests);
  } catch (e) {
    console.error(e);
  }
}

execute_request();
