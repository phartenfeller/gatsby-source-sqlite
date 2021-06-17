const queryDb = require('./src/db');
const createSqliteNodes = require('./src/create-sqlite-nodes');

exports.sourceNodes = async (
  { actions, store, createNodeId, cache, reporter },
  configOptions
) => {
  const { createNode } = actions;
  const {
    fileName,
    queries,
    cacheQueryResults = false,
    cacheTransformationByRowcount = false,
  } = configOptions;

  const queryResults = await queryDb({
    fileName,
    queries,
    reporter,
    cache,
    cacheQueryResults,
  });
  try {
    const sqlData = queries.map((query, index) =>
      Object.assign({}, query, { __sqlResult: queryResults[index] })
    );

    await Promise.all(
      sqlData.map((sqlResult, _, arr) =>
        createSqliteNodes(sqlResult, arr, {
          createNode,
          store,
          createNodeId,
          cache,
          reporter,
          cacheTransformationByRowcount,
        })
      )
    );
  } catch (e) {
    reporter.error(`Error while sourcing data with gatsby-source-sqlite`, e);
  }
};
