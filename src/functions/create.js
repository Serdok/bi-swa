import { app } from "@azure/functions";
import pg from "pg";

const mock = {
  objects: [
    {
      name: "G4BI_Item",
      type: "VIEW",
      subType: "NONE",
      schema: {
        id: "http://schema.infor.com/json-schema/G4BI_Item.json",
        title: "G4BI_Item",
        required: [],
        additionalProperties: false,
        $schema: "http://json-schema.org/draft-06/schema#",
        properties: {
          CompanyNumber: {
            type: "integer",
            maximum: "9223372036854776000",
            "x-position": 1,
            xposition: 1,
          },
          Item: {
            type: "string",
            maxLength: 47,
            "x-position": 2,
            xposition: 2,
          },
          ItemDescription: {
            type: "string",
            maxLength: 255,
            "x-position": 3,
            xposition: 3,
          },
          ItemGroup: {
            type: "string",
            maxLength: 6,
            "x-position": 4,
            xposition: 4,
          },
        },
      },
      properties: {
        AdditionalProperties: {
          infor_datalake_view: {
            formatVersion: 1,
            createViewRequest: {
              formatVersion: 1,
              originalStatement:
                "CREATE VIEW G4BI_Item AS SELECT compnr AS CompanyNumber, item AS Item, dsca AS ItemDescription, citg AS ItemGroup FROM LN_tcibd001",
              referencedTables: [
                {
                  formatVersion: 1,
                  namespace: {
                    companyId: "default",
                    databaseName: "default",
                    schemaName: "default",
                  },
                  name: "ln_tcibd001",
                  variationId: 0,
                  referencedColumns: ["item", "dsca", "citg", "compnr"],
                },
              ],
            },
          },
        },
      },
      lastUpdatedOn: 1706538158517,
      lastUpdatedBy: "Compass",
    },
  ],
  count: 1,
};

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

      const query = createTable(body);

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

        context.log(query);
        const result = await client.query(query);

        client.release();
        return { status: 200, jsonBody: result };
      } catch (err) {
        context.error(err.message);
        return { status: 500, body: err };
      }
    } catch (err) {
      context.error(err.message);
      return { status: 400, body: err };
    }
  },
});
