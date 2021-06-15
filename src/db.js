const sqlite3 = require('sqlite3');

let db = null;

function getDb(fileName, reporter) {
  return new Promise((resolve, reject) => {
    if (db) resolve(db);

    db = new sqlite3.Database(fileName, (err) => {
      if (err) {
        reporter.panic(`Error opening DB => ${err.message}`);
        reject(err);
      }
    });
    resolve(db);
  });
}

async function queryRows({ query, db, reporter }) {
  return new Promise((resolve, reject) => {
    db.all(query, [], (err, rows) => {
      if (err) {
        reporter.error(`Could net execute Query: "${query}".\nError: ${err.message}`);
        reject(err);
      }
      resolve(rows);
    });
  });
}

async function queryDb(fileName, queries, reporter) {
  const db = await getDb(fileName, reporter);

  let promises = queries.map(({ statement }) =>
    queryRows({ query: statement, db, reporter })
  );

  return Promise.all(promises);
}

module.exports = queryDb;
