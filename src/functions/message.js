import { app } from '@azure/functions';
import pg from 'pg';

function createQuery(data, tableName) {
    return data.map(row => {
        const obj = Object.entries(row).reduce((acc, curr) => {
          const [key, value] = curr;
          acc.columns.push(key);
          acc.values.push(value);
          return acc;
        }, {columns: [], values: []});
      
        const columns = obj.columns.map(column => `"${column}"`)
        const placeholders = obj.values.map((value, idx) => `$${idx + 1}`)
        return { text: `insert into ${tableName} (${columns}) values (${placeholders})`, values: obj.values};
    });
}

app.http('message', {
    methods: ['POST'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        context.log(`Http function processed request for url "${request.url}"`);

        const config = {
            host: process.env["PGHOST"],
            port: process.env["PGPORT"],
            user: process.env["PGUSER"],
            password: process.env["PGPASSWORD"],
            database: process.env["PGDATABASE"],
            ssl: true,
        }

        const tableName = request.query.get('table');
        if (tableName === null) {
            return { status: 400, jsonBody: { message: 'Table name missing in query' }};
        }

        // try {
        //     const body = await request.json();

        //     const queries = createQuery(body, tableName);

        //     // Connect to the database
        //     const pool = new pg.Pool(config);
        //     const client = await pool.connect();

        //     // Prepare queries
        //     const commands = queries.map(query => client.query(query.text, query.values));
        //     const results = await Promise.all(commands);

        //     return { status: 200, jsonBody: results, };
        // } catch (err) {
        //     context.error(err.message);
        //     return { status: 500, body: err, };
        // }
        
        try {
            // Connect to the database
            const pool = new pg.Pool(config);
            const client = await pool.connect();

            const table = request.query.get('table') || 'G4BI_Items';
            const body = await request.json();

            let hasError = false;

            body.forEach(async (record) =>
                {
                    let sqlQueryStart = `INSERT INTO ${table} (`
                    let sqlQueryEnd = 'VALUES ('
                    let valuesForQuery = []
                    Object.keys(record).forEach((key,index)=> 
                    {
                        let valueField = record[key] 
                        if (typeof valueField === 'string')
                        {
                        //   valueField = "'" + valueField.replace("'", "''") + "'"
                        }
                        if (index === 0)
                        {
                            sqlQueryStart += `"${key}"`
                            sqlQueryEnd += '$' + (index + 1).toString()
                            valuesForQuery.push(valueField)
                        }
                        else
                        {
                            sqlQueryStart += ', ' + `"${key}"`  
                            sqlQueryEnd += ', ' + '$' + (index + 1).toString()
                            valuesForQuery.push(valueField)
                        }
                    })
                  sqlQueryStart += ') '
                  sqlQueryEnd += ') '
                  let sqlText = sqlQueryStart + sqlQueryEnd + 'RETURNING *'
                  context.log(sqlText, "-", valuesForQuery)
                      try {
                        const queryResult = await client.query(sqlText, valuesForQuery);
              
                context.log(queryResult);
                      } catch (err) {
                        context.error(err.message);
                        hasError = true;
                      }
                })

            // body.forEach(async (record) => {
            //     sqlQueryStart = 'INSERT INTO ('
            //     sqlQueryEnd = 'VALUES ('
            //     Object.keys(record).forEach((key,index)=> 
            //     {
            //       valueField = record[key] 
            //     if (typeof valueField === 'string')
            //     {
            //       valueField = "'" + valueField.replace("'", "''") + "'"
            //     }
            //     if (index === 0)
            //     {
            //         sqlQueryStart += key
            //         sqlQueryEnd += valueField
            //     }
            //     else
            //     {
            //         sqlQueryStart += ', ' + key  
            //         sqlQueryEnd += ', ' + valueField
            //     }
             
            //     context.log(key, "-", index, "-", valueField)
            //     })
            //   sqlQueryStart += ') '
            //   sqlQueryEnd += '); '

            //     // Execute query
            //     context.log(sqlQueryStart + sqlQueryEnd)
            //     const queryResult = await client.query(sqlQueryStart + sqlQueryEnd, );
              
            //     context.log(queryResult);
            // })

            
            // Release connection
            client.release();

            if (hasError) {
                return { code: 400, body: 'error' };
            }
            
            return { body: `done` };
        } catch (err) {
            context.error(err.message);
        }

        return { code: 400, body: `error` };
    }
})