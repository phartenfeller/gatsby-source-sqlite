# gatsby-source-sqlite

**This is just forked from [malcolm-kee](https://github.com/malcolm-kee)'s awesome [gatsby-source-mysql](https://github.com/malcolm-kee/gatsby-source-mysql) plugin. I just changed query logic and replaced "mysql" with "sqlite" everywhere. So all credits to him!!!**

[![version](https://img.shields.io/npm/v/gatsby-source-sqlite.svg)](https://www.npmjs.com/package/gatsby-source-sqlite) ![license](https://img.shields.io/npm/l/gatsby-source-sqlite.svg)

Source plugin for pulling data into Gatsby from a SQLite database file.

## How to use

```javascript
// In your gatsby-config.js
module.exports = {
  plugins: [
    {
      resolve: `gatsby-source-sqlite`,
      options: {
        fileName: './data/mydb.sqlite',
        queries: [
          {
            statement: 'SELECT * FROM country',
            idFieldName: 'Code',
            name: 'country'
          }
        ]
      }
    }
    // ... other plugins
  ]
};
```

And then you can query via GraphQL with the type `allSqlite<Name>` where `<Name>` is the `name` for your query.

Below is a sample query, however, it is probably different from yours as it would dependent on your configuration and your SQL query results.

Use [GraphiQL](https://www.gatsbyjs.org/docs/introducing-graphiql/) to explore the available fields.

```graphql
query {
  allSqliteCountry {
    edges {
      node {
        Code
        Name
        Population
      }
    }
  }
}
```

### Multiple Queries

When you have multiple queries, add another item in the `queries` option with different `name`.

```javascript
// In your gatsby-config.js
module.exports = {
  plugins: [
    {
      resolve: `gatsby-source-sqlite`,
      options: {
        fileName: './data/mydb.sqlite',
        queries: [
          {
            statement: 'SELECT * FROM country',
            idFieldName: 'Code',
            name: 'country'
          },
          {
            statement: 'SELECT * FROM city',
            idFieldName: 'ID',
            name: 'city'
          }
        ]
      }
    }
    // ... other plugins
  ]
};
```

### Joining Queries

It's possible to join the results of the queries by providing `parentName`, `foreignKey`, and `cardinality` to the query object.

```javascript
// In your gatsby-config.js
module.exports = {
  plugins: [
    {
      resolve: `gatsby-source-sqlite`,
      options: {
        fileName: './data/mydb.sqlite',
        queries: [
          {
            statement: 'SELECT * FROM country',
            idFieldName: 'Code',
            name: 'country'
          },
          {
            statement: 'SELECT * FROM city',
            idFieldName: 'ID',
            name: 'city',
            parentName: 'country',
            foreignKey: 'CountryCode',
            cardinality: 'OneToMany'
          }
        ]
      }
    }
    // ... other plugins
  ]
};
```

In the example above, `country` and `city` is one-to-many relationship (one country to multiple cities), and they are joined with `country.Code = city.CountryCode`.

With the configuration above, you can query a country joined with all the related cities with

```graphql
query {
  allSqliteCountry {
    edges {
      node {
        Code
        Name
        Population
        cities {
          Name
        }
      }
    }
  }
}
```

It also works the other way, i.e. you can query the country when getting the city

```graphql
query {
  allSqliteCity {
    edges {
      node {
        Name
        country {
          Name
        }
      }
    }
  }
}
```

### Download Image for Image Processing

If your queries stores the remote url for image and you would like to utilize image processing of Gatsby, provide `remoteImageFieldNames` to the query object.

> Make sure you've installed both [`gatsby-plugin-sharp`][gatsby-plugin-sharp] and [`gatsby-transform-sharp`][gatsby-transformer-sharp] packages and add them to your `gatsby-config.js`.

For example, assuming you have a `actor` table where the `profile_url` column stores the remote image url, e.g. `'https://cdn.pixabay.com/photo/2014/07/10/11/15/balloons-388973_1280.jpg'`.

```javascript
// In your gatsby-config.js
module.exports = {
  plugins: [
    `gatsby-plugin-sharp`,
    `gatsby-transformer-sharp`,
    {
      resolve: `gatsby-source-sqlite`,
      options: {
        fileName: './data/mydb.sqlite',
        queries: [
          {
            statement: 'SELECT * FROM actor',
            idFieldName: 'actor_id',
            name: 'actor',
            remoteImageFieldNames: ['profile_url']
          }
        ]
      }
    }
    // ... other plugins
  ]
};
```

Then you can query all the images like below. (Note that you have to filter `null` value for the records whose `profile_url` is empty).

```jsx
import React from 'react';
import { useStaticQuery, graphql } from 'gatsby';
import Img from 'gatsby-image';

export const SqlImage = () => {
  const data = useStaticQuery(graphql`
    {
      allSqliteActor {
        edges {
          node {
            SqliteImage {
              childImageSharp {
                fluid(maxWidth: 300) {
                  ...GatsbyImageSharpFluid
                }
              }
            }
          }
        }
      }
    }
  `);

  const images = data.allSqliteActor.edges
    .filter(edge => !!edge.node.SqliteImage)
    .map(edge => edge.node.SqliteImage.childImageSharp.fluid);

  return (
    <div>
      {images.map((img, index) => (
        <Img fluid={img} key={index} />
      ))}
    </div>
  );
};
```

If you have multiple columns with image url, pass down multiple values to `remoteImageFieldNames` and use `SqliteImages` in your graphql query, which will be an array of images.

## Plugin options

- **connectionDetails** (required): options when establishing the connection. Refer to [`Sqlite` connection options](https://www.npmjs.com/package/Sqlite#connection-options)
- **queries** (required): an array of object for your query. Each object could have the following fields:

| Field                   | Required? | Description                                                                                                                                                                                |
| ----------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `statement`             | Required  | the SQL query statement to be executed.                                                                                                                                                    |
| `idFieldName`           | Required  | column that is unique for each record. This column must be returned by the `statement`. The name `id` is already reserverd and not allowed here!!!                                         |
| `name`                  | Required  | name for the query. Will impact the value for the graphql type                                                                                                                             |
| `parentName`            | Optional  | name for the parent entity. In a one-to-many relationship, this field should be specified on the child entity (entity with many records).                                                  |
| `foreignKey`            | Optional  | foreign key to join the parent entity.                                                                                                                                                     |
| `cardinality`           | Optional  | the relationship between the parent and this entity. Possible values: `"OneToMany"`, `"OneToOne"`. Default to `"OneToMany"`. (Note: many-to-many relationship is currently not supported.) |
| `remoteImageFieldNames` | Optional  | columns that contain image url which you want to download and utilize Gatsby image processing capability.                                                                                  |

[raise-issue]: https://github.com/malcolm-kee/gatsby-source-sqlite/issues/new
[gatsby-plugin-sharp]: https://www.gatsbyjs.org/packages/gatsby-plugin-sharp/
[gatsby-transformer-sharp]: https://www.gatsbyjs.org/packages/gatsby-transformer-sharp/
