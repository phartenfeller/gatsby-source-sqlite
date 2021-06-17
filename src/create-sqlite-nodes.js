const createNodeHelpers = require('gatsby-node-helpers').default;
const { createRemoteFileNode } = require('gatsby-source-filesystem');
const pluralize = require('pluralize');

let G_REPORTER;
const G_CACHE = {};

/* https://youmightnotneed.com/lodash/#chunk */
const chunk = (arr, chunkSize = 1, cache = []) => {
  const tmp = [...arr];
  if (chunkSize <= 0) return cache;
  while (tmp.length) cache.push(tmp.splice(0, chunkSize));
  return cache;
};

const BATCH_SIZE = 3;

const { createNodeFactory, generateNodeId } = createNodeHelpers({
  typePrefix: 'sqlite',
});

function reduceChildFields(childEntities, nodeId, name) {
  let childFields = {};

  childEntities.forEach(
    ({ name: childName, idFieldName: childIdFieldName, parents, __sqlResult }) => {
      const { foreignKey, cardinality = 'OneToMany' } = parents.find(
        (p) => p.parentName === name
      );

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
  { parentName, foreignKey, childEntities, idFieldName, name }
) {
  return __sqlResult.map((result) => {
    const nodeId = result[idFieldName];
    const parentField =
      parentName && foreignKey
        ? {
            [`${parentName}___NODE`]: generateNodeId(parentName, result[foreignKey]),
          }
        : {};

    const childFields = reduceChildFields(childEntities, nodeId, name);

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
  {
    createNode,
    store,
    createNodeId,
    cache,
    cacheTransformationByRowcount,
    cacheKeyFileNodes,
    cacheKeyNodes,
  }
) {
  const SqliteNode = createNodeFactory(name);
  const sqlNode = SqliteNode(node);

  const remoteNodes = await Promise.all(
    remoteImageFieldNames
      .filter((field) => !!node[field])
      .map(async (field) => {
        try {
          const fileNodeData = {
            url: node[field],
            parentNodeId: sqlNode.id,
            store,
            createNode,
            createNodeId,
            cache,
          };
          if (cacheTransformationByRowcount) {
            G_CACHE[cacheKeyFileNodes].push(fileNodeData);
          }
          return createRemoteFileNode(fileNodeData);
        } catch (e) {
          if (typeof e === 'string') {
            G_REPORTER.error(`Error when getting image ${node[field]}: ${e.toString()}`);
          } else {
            G_REPORTER.error(`Error when getting image ${node[field]}`, e);
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

  if (cacheTransformationByRowcount) {
    G_CACHE[cacheKeyNodes].push(sqlNode);
  }

  await createNode(sqlNode);
}

async function createSqliteNodes(
  {
    statement,
    name,
    __sqlResult,
    idFieldName,
    parentName,
    foreignKey,
    remoteImageFieldNames = [],
  },
  queries,
  { createNode, store, createNodeId, cache, reporter, cacheTransformationByRowcount }
) {
  G_REPORTER = reporter;
  reporter.info(`Transformation`);
  reporter.info(`Object => ${name} - rows => ${__sqlResult.length}`);
  const cacheKeyRowcount = `rowcount-${name}`;
  const cacheKeyNodes = `data-${name}`;
  const cacheKeyFileNodes = `node-${name}`;

  if (cacheTransformationByRowcount) {
    G_CACHE[cacheKeyNodes] = [];
    G_CACHE[cacheKeyFileNodes] = [];

    const cachedRowcount = await cache.get(cacheKeyRowcount);
    reporter.info(`cachedRowcount => ${cachedRowcount}`);

    if (cachedRowcount === __sqlResult.length) {
      let promises = [];

      const fileNodes = await cache.get(cacheKeyFileNodes);
      for (let i = 0; i < fileNodes.length; i += 1) {
        delete fileNodes[i].internal.owner;
        promises.push(createRemoteFileNode(fileNodes[i]));
      }
      reporter.info(`fileNodes => ${fileNodes.length}`);

      const nodes = await cache.get(cacheKeyNodes);
      for (let i = 0; i < nodes.length; i += 1) {
        delete nodes[i].internal.owner;
        promises.push(createNode(nodes[i]));
      }
      reporter.info(`nodes => ${nodes.length}`);

      return promises;
    }
  }

  // get all types that have the current one as parent
  const childEntities = queries.filter(({ parents }) => {
    if (!parents || !parents.length || parents.length === 0) {
      return;
    }

    return (
      parents.filter(({ parentName }) => parentName && parentName === name).length > 0
    );
  });

  if (Array.isArray(__sqlResult)) {
    const mapped = mapSqlResults(
      __sqlResult,
      { foreignKey, parentName, childEntities, idFieldName, name },
      childEntities
    );

    const sqlNodesChunks = chunk(mapped, BATCH_SIZE);

    let promises = [];

    for (let i = 0; i < sqlNodesChunks.length; i += 1) {
      promises.push(
        sqlNodesChunks[i].map((node) =>
          createSqliteNode(
            node,
            { name, remoteImageFieldNames },
            {
              createNode,
              store,
              createNodeId,
              cache,
              cacheTransformationByRowcount,
              cacheKeyFileNodes,
              cacheKeyNodes,
            }
          )
        )
      );
    }

    await Promise.all(promises);

    if (cacheTransformationByRowcount) {
      reporter.info(`Caching data for ${statement}...`);
      await cache.set(cacheKeyRowcount, __sqlResult.length);
      await cache.set(cacheKeyFileNodes, G_CACHE[cacheKeyFileNodes]);
      delete G_CACHE[cacheKeyFileNodes];
      await cache.set(cacheKeyNodes, G_CACHE[cacheKeyNodes]);
      delete G_CACHE[cacheKeyNodes];
    }

    return;

    // for (let sqlNodes of sqlNodesChunks) {
    //   await Promise.all(
    //     sqlNodes.map((node) =>
    //       createSqliteNode(
    //         node,
    //         { name, remoteImageFieldNames },
    //         { createNode, store, createNodeId, cache, reporter }
    //       )
    //     )
    //   );
    // }
  }
}

module.exports = createSqliteNodes;
