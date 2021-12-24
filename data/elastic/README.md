## Executing SimSearch against Elastisearch data sources.

This collection of requests refers to executing SimSearch functionalities against attribute data available strictly through Elastisearch REST API.

It assumes a basic authentication is employed (username/password credentials) for connecting to the Elastisearch REST API.

In case of Elasticsearch indices with nested fields, their inner sub-fields can be specified as data sources to SimSearch and queryable attributes in similarity search requests using the [dot notation](https://www.elastic.co/guide/en/elasticsearch/reference/current/properties.html), e.g., "manager.name".
