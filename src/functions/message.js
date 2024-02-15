const { app } = require('@azure/functions');
const pg = require('pg');

function composeInsertString(arr, tableName) {
    return arr.map((value, idx) => {

    });
}

app.http('message', {
    methods: ['POST'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        context.log(`Http function processed request for url "${request.url}"`);
        // const name = request.query.get('name') || await request.text() || 'world';

        // TODO: Add config from env variables
        const config = {
            host: process.env["PGHOST"],
            port: process.env["PGPORT"],
            user: process.env["PGUSER"],
            password: process.env["PGPASSWORD"],
            database: process.env["PGDATABASE"],
            ssl: true,
        }
        
        
        try {
            // Connect to the database
            const pool = new pg.Pool(config);
            const client = await pool.connect();

            const body = await request.json();

            let hasError = false;
            body.forEach(async (record) =>
                {
                    sqlQueryStart = 'INSERT INTO G4BI_Items ('
                    sqlQueryEnd = 'VALUES ('
                    valuesForQuery = []
                    Object.keys(record).forEach((key,index)=> 
                    {
                        valueField = record[key] 
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
                  sqlText = sqlQueryStart + sqlQueryEnd + 'RETURNING *'
                  console.log(sqlText, "-", valuesForQuery)
                      try {
                        const queryResult = await client.query(sqlText, valuesForQuery);
              
                console.log(queryResult);
                      } catch (err) {
                        console.error(err.message);
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
             
            //     console.log(key, "-", index, "-", valueField)
            //     })
            //   sqlQueryStart += ') '
            //   sqlQueryEnd += '); '

            //     // Execute query
            //     console.log(sqlQueryStart + sqlQueryEnd)
            //     const queryResult = await client.query(sqlQueryStart + sqlQueryEnd, );
              
            //     console.log(queryResult);
            // })

            
            // Release connection
            client.release();
            return { body: hasError ? `error` : `done` };
        } catch (err) {
            console.error(err.message);
        }

        return { body: `error` };
    }
});
