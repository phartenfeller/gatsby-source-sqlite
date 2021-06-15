const queryDb = require('./src/db');
const createSqliteNodes = require('./src/create-sqlite-nodes');

exports.sourceNodes = async (
  { actions, store, createNodeId, cache, reporter },
  configOptions
) => {
  const { createNode } = actions;
  const { fileName, queries } = configOptions;

  const queryResults = await queryDb(fileName, queries, reporter);
  try {
    const sqlData = queries.map((query, index) =>
      Object.assign({}, query, { __sqlResult: queryResults[index] })
    );

    await Promise.all(
      sqlData.map((sqlResult, _, sqlResults) =>
        createSqliteNodes(sqlResult, sqlResults, {
          createNode,
          store,
          createNodeId,
          cache,
          reporter,
        })
      )
    );
  } catch (e) {
    reporter.error(`Error while sourcing data with gatsby-source-sqlite`, e);
  }
};
