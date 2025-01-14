const createNodeHelpers = require('gatsby-node-helpers').default;
const { createRemoteFileNode } = require('gatsby-source-filesystem');
const pluralize = require('pluralize');
const { chunk } = require('lodash');

const BATCH_SIZE = 3;

const { createNodeFactory, generateNodeId } = createNodeHelpers({
  typePrefix: 'sqlite',
});

function reduceChildFields(childEntities, nodeId) {
  let childFields = {};

  childEntities.forEach(
    ({
      name: childName,
      idFieldName: childIdFieldName,
      foreignKey,
      cardinality = 'OneToMany',
      __sqlResult,
    }) => {
      const childIds = __sqlResult
        .filter((child) => child[foreignKey] === nodeId)
        .map((child) => generateNodeId(childName, child[childIdFieldName]));

      if (cardinality === 'OneToMany') {
        childFields[`${pluralize.plural(childName)}___NODE`] = childIds;
      } else {
        childFields[`${pluralize.singular(childName)}___NODE`] = childIds[0];
      }
    }
  );

  return childFields;
}

function mapSqlResults(
  __sqlResult,
  { parentName, foreignKey, childEntities, idFieldName }
) {
  return __sqlResult.map((result) => {
    const nodeId = result[idFieldName];
    const parentField =
      parentName && foreignKey
        ? {
            [`${parentName}___NODE`]: generateNodeId(parentName, result[foreignKey]),
          }
        : {};

    const childFields = reduceChildFields(childEntities, nodeId);

    return Object.assign(
      {},
      result,
      {
        id: nodeId,
      },
      parentField,
      childFields
    );
  });
}

async function createSqliteNode(
  node,
  { name, remoteImageFieldNames },
  { createNode, store, createNodeId, cache, reporter }
) {
  const SqliteNode = createNodeFactory(name);
  const sqlNode = SqliteNode(node);

  const remoteNodes = await Promise.all(
    remoteImageFieldNames
      .filter((field) => !!node[field])
      .map(async (field) => {
        try {
          return await createRemoteFileNode({
            url: node[field],
            parentNodeId: sqlNode.id,
            store,
            createNode,
            createNodeId,
            cache,
          });
        } catch (e) {
          if (typeof e === 'string') {
            reporter.error(`Error when getting image ${node[field]}: ${e.toString()}`);
          } else {
            reporter.error(`Error when getting image ${node[field]}`, e);
          }
        }
      })
  );

  // filter out nodes which fail
  const imageNodes = remoteNodes.filter(Boolean);

  if (remoteImageFieldNames.length === 1) {
    if (imageNodes.length > 0) {
      sqlNode.sqliteImage___NODE = imageNodes[0].id;
    }
  }

  sqlNode.sqliteImages___NODE = imageNodes.map((imageNode) => imageNode.id);

  await createNode(sqlNode);
}

async function createSqliteNodes(
  { name, __sqlResult, idFieldName, parentName, foreignKey, remoteImageFieldNames = [] },
  allSqlResults,
  { createNode, store, createNodeId, cache, reporter }
) {
  const childEntities = allSqlResults.filter(
    ({ parentName }) => !!parentName && parentName === name
  );

  if (Array.isArray(__sqlResult)) {
    const sqlNodesChunks = chunk(
      mapSqlResults(
        __sqlResult,
        { foreignKey, parentName, childEntities, idFieldName },
        childEntities
      ),
      BATCH_SIZE
    );

    for (let sqlNodes of sqlNodesChunks) {
      await Promise.all(
        sqlNodes.map((node) =>
          createSqliteNode(
            node,
            { name, remoteImageFieldNames },
            { createNode, store, createNodeId, cache, reporter }
          )
        )
      );
    }
  }
}

module.exports = createSqliteNodes;
