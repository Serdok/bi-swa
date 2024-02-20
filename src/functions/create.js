import { app } from "@azure/functions";
import pg from "pg";


function composeQueryFromView(view) {
  const table = view.name.toLowerCase();
  const properties = view.schema.properties;

  const columns = Object.entries(properties).reduce((acc, curr) => {
    const [key, value] = curr;
    const type = value.type.toLowerCase() || "string";

    // TODO: Find a scalable (?) way to map types
    switch (type) {
      case "integer":
        acc.push({ name: key.toLowerCase(), type: "bigint" });
        break;
      case "number":
        acc.push({ name: key.toLowerCase(), type: "numeric" });
        break;
      case "string":
      default:
        acc.push({
          name: key.toLowerCase(),
          type: `varchar(${value.maxLength || 2000})`,
        });
        break;
    }

    return acc;
  }, []);

  const text = columns.map((col) => `${col.name} ${col.type}`);
  return `drop table if exists ${table}; create table ${table} (${text});`;
}

function createQuery(result) {
  return result.objects.map((view) => {
    return composeQueryFromView(view);
  });
}

function createTable(results) {
  const tableName = results.objects[0].name.toLowerCase();
  const schemaProperties = results.objects[0].schema.properties;
  const fields = Object.keys(schemaProperties);
  let firstLoop = true;
  let sqlQuery = `DROP TABLE IF EXISTS ${tableName}; CREATE TABLE "${tableName}" (`;
  let tmpQuery = "";
  let dataType = "";
  let maxLength;

  fields.forEach((elem) => {
    switch (schemaProperties[elem].type) {
      case "integer":
        dataType = "bigint";
        break;
      case "string":
        maxLength = schemaProperties[elem].maxLength;
        if (Number.isInteger(maxLength)) {
          dataType = `varchar(${maxLength})`;
        } else {
          dataType = "varchar(2000)";
        }
        break;
      case "number":
        dataType = "numeric";
        break;
      default:
        dataType = "varchar(2000)";
        break;
    }
    tmpQuery = `"${elem}" ${dataType}`;
    if (firstLoop) {
      sqlQuery += tmpQuery;
      firstLoop = false;
    } else {
      sqlQuery += `, ${tmpQuery}`;
    }
  });
  sqlQuery += ");";
  return sqlQuery;
}

app.http("create", {
  methods: ["POST"],
  authLevel: "anonymous",
  handler: async (request, context) => {
    context.log(`Http function processed request for url "${request.url}"`);

    try {
      const body = await request.json();

      const query = composeQueryFromView(body);
      
      // const queries = createQuery(body);      

      try {
        // Connect to the database
        const config = {
          host: process.env["PGHOST"],
          port: process.env["PGPORT"],
          user: process.env["PGUSER"],
          password: process.env["PGPASSWORD"],
          database: process.env["PGDATABASE"],
          ssl: true,
        };

        const pool = new pg.Pool(config);
        const client = await pool.connect();

        
        // context.log(queries);
        // const result = await client.query(query);
        // const commands = queries.map((query) => client.query(query));
        // const result = await Promise.all(commands);

        context.log(query);
        const result = await client.query(query);
      
        client.release();
        return { status: 200, jsonBody: result };
      } catch (err) {
        // Database error
        context.error(err.message);
        return { status: 500, body: err };
      }
    } catch (err) {
      // Validation error
      context.error(err.message);
      return { status: 400, body: err };
    }
  },
});
