import { app } from '@azure/functions';
import pg from "pg";

app.serviceBusQueue('inserter', {
    queueName: 'db-requests',
    connection: 'AzureWebServiceBus',
    cardinality: 'many',
    handler: async (messages, context) => {
        context.log(`Service bus trigger: ${messages.length} rows to insert`)
        const start = Date.now();

        const config = {
            host: process.env.AZURE_POSTGRESQL_HOST,
            port: Number(process.env.AZURE_POSTGRESQL_PORT),
            user: process.env.AZURE_POSTGRESQL_USER,
            password: process.env.AZURE_POSTGRESQL_PASSWORD,
            database: process.env.AZURE_POSTGRESQL_DATABASE,
            ssl: process.env.AZURE_POSTGRESQL_SSL,
            max: 20,
            application_name: 'Azure Functions - Service Bus Trigger',
            // log: (...args) => { context.trace(...args); },
            idleTimeoutMillis: 0, // disable disconnect of idle client
            // statement_timeout: 10000, // 10s
            // allowExitOnIdle: true,
          };

          const pool = new pg.Pool(config);
          const queries = messages.map(query => pool.query(query.text, query.values));

          try {
            const result = await Promise.allSettled(queries);

            const fulfilled = result.filter(res => res.status === 'fulfilled');
            const rejected = result.filter(res => res.status === 'rejected');

            if (rejected.length > 0) {
              // TODO: Put messages back in queue to be processed again?
              context.error(JSON.stringify(rejected));
            }

            await pool.end();

            context.log(`${fulfilled.length} rows inserted in ${Date.now() - start} ms`);
          } catch (e) {
            context.error(e);
            throw e;
          }
    }
});
