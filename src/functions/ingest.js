import { app } from "@azure/functions";
import pg from "pg";

function createQuery(data, tableName) {
  return data.map((row) => {
    const obj = Object.entries(row).reduce(
      (acc, curr) => {
        const [key, value] = curr;
        acc.columns.push(key);
        acc.values.push(value);
        return acc;
      },
      { columns: [], values: [] }
    );

    const columns = obj.columns.map((column) => `${column.toLowerCase()}`);
    const placeholders = obj.values.map((value, idx) => `$${idx + 1}`);
    return {
      text: `insert into ${tableName.toLowerCase()} (${columns}) values (${placeholders})`,
      values: obj.values,
    };
  });
}

app.http("ingest", {
  methods: ["POST"],
  authLevel: "anonymous",
  handler: async (request, context) => {
    context.log(`Http function processed request for url "${request.url}"`);

    const config = {
      host: process.env["PGHOST"],
      port: process.env["PGPORT"],
      user: process.env["PGUSER"],
      password: process.env["PGPASSWORD"],
      database: process.env["PGDATABASE"],
      ssl: true,
    };

    const tableName = request.query.get("table");
    if (tableName === null) {
      return {
        status: 400,
        jsonBody: { message: "Table name missing in query" },
      };
    }

    try {
      const body = await request.json();
      const queries = createQuery(body, tableName);

      // Connect to the database
      const pool = new pg.Pool(config);

      // Prepare queries - ignore errors and proceeed
      const commands = queries.map((query) =>
        pool.query(query.text, query.values)
      );
      const results = await Promise.allSettled(commands);

      const rejected = results
        .map((result, idx) => ({ row: idx + 1, result }))
        .filter((result) => result.result.status === "rejected");
      context.log(`Done, ${results.length - rejected.length} rows processed`);

      const messages = rejected.map((rej) => ({
        row: rej.row,
        message: rej.result.reason.message,
      }));

      if (rejected.length > 0) {
        context.error(messages);
        return { status: 206, jsonBody: messages };
      }

      return { status: 201, jsonBody: {}};
    } catch (err) {
      context.error(err.message);
      return { status: 500, jsonBody: err };
    }
  },
});
