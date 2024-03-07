import { app, output } from "@azure/functions";
import { ServiceBusClient } from "@azure/service-bus";

// const queue = output.serviceBusQueue({
//   queueName: "db-requests",
//   connection: "AzureWebServiceBus",
// });

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

async function sendBatch(context, data) {
  const client = new ServiceBusClient(process.env.AzureWebServiceBus);
  const sender = client.createSender("db-requests");

  try {
    const messages = data.map((d) => ({
      contentType: "application/json",
      body: d,
    }));

    let batch = await sender.createMessageBatch();
    for (const message of messages) {
      if (!batch.tryAddMessage(message)) {
        // Current batch is full, send
        context.log(
          `Batch full, sending ${batch.count} messages (${batch.sizeInBytes} MB)`
        );
        await sender.sendMessages(batch);
        batch = await sender.createMessageBatch();

        if (!batch.tryAddMessage(message)) {
          throw new Error("Message too big to fit in a batch");
        }
      }
    }

    context.log(`Sending ${batch.count} messages (${batch.sizeInBytes} MB)`);
    await sender.sendMessages(batch);
  } finally {
    await sender.close();
    await client.close();
  }
}

app.http("enqueue", {
  methods: ["POST"],
  authLevel: "anonymous",
  // extraOutputs: [queue],
  handler: async (request, context) => {
    context.log(`Http function processed request for url "${request.url}"`);

    const tableName = request.query.get("table");
    if (tableName === null) {
      return {
        status: 400,
        jsonBody: { message: "Table name missing in query" },
      };
    }

    try {
      const data = await request.json();

      // request.json() doesn't output array?
      const queries = createQuery(JSON.parse(data), tableName);
      context.log(`Enqueued ${queries.length} messages`);

      await sendBatch(context, queries);
      return { status: 201 };
    } catch (e) {
      context.error(e);
      return { status: 500, jsonBody: e };
    }
  },
});
