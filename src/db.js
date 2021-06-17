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

async function queryDb({ fileName, queries, reporter, cache, cacheQueryResults }) {
  const db = await getDb(fileName, reporter);

  let promises = queries.map(async ({ statement }) => {
    const cacheKey = `query-${statement}`;
    reporter.info(`cacheQueryResults = ${cacheQueryResults}`);
    if (cacheQueryResults) {
      const cachedRes = await cache.get(cacheKey);
      reporter.info(`cachedRes = ${!!cachedRes}`);
      if (cachedRes) return cachedRes;
    }

    const dbRes = await queryRows({ query: statement, db, reporter });
    await cache.set(cacheKey, dbRes);
    return dbRes;
  });

  return Promise.all(promises);
}

module.exports = queryDb;
