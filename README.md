# SimSearch

## Overview

SimSearch is a Java library providing functions for combined similarity search against multi-faceted entities, i.e., datasets with different types of attributes (textual/categorical, numerical, spatial, temporal, etc.). The queries enable multi-attribute similarity search for data exploration and may involve different similarity measures per attribute (Jaccard, Euclidean, etc.). This library builts specialized indexes for each specific attribute type. It currently supports the following operations:

- Categorical (set-valued) similarity search: return data elements with the highest similarity score to the given query set of keywords.
- Numerical similarity search: return data elements with the highest similarity score to the given query (numerical) value.
- Spatial similarity search: implements k-nearest neighbor search and return data elements closest to the given query (point) location.
- Top-k rank aggregation: find top-k results across all examined attributes and issue each result ranked by an aggregate similarity score.

Attribute data values may come from diverse data sources:

- CSV files that will be ingested and indices will be constructed in memory (e.g., R-trees for spatial locations, B-trees for numerical values, inverted indices for sets of textual values).
- Tables in a [PostgreSQL](https://www.postgresql.org/) database (with [PostGIS extension](https://postgis.net/) if geometries are also stored in a spatial column).
- Data available from REST APIs, like JSON data hosted in [Elasticsearch](https://www.elastic.co/elasticsearch).

SimSearch can be deployed either as a standalone Java application or as a RESTful web service.

## Documentation

Javadoc is available [here](https://smartdatalake.github.io/simsearch/).

## Usage

**Step 1**. Download or clone the project:
```sh
$ git clone https://github.com/smartdatalake/simsearch.git
```

**Step 2**. Open terminal inside root folder and compile by running:
```sh
$ mvn clean package spring-boot:repackage
```
**Step 3**. Edit the parameters for the various data sources and their queryable attributes in the `sources.json` file.

**Step 3**. Edit the parameters in the `search.json` file for the various attributes involved in the top-k similarity search query.

## Standalone execution

To invoke SimSearch in standalone mode as a Java application, run the executable:
```sh
$ java -jar target/simsearch-0.0.1-SNAPSHOT.jar
```

Next, choose a number corresponding to a functionality you want to apply:

(1): MOUNT SOURCES -> Specifies the queryable attributes and (if necessary) builts suitable indices on their values. The user must also specify the path to a JSON file (as in `sources.json.example` file or [`data/gdelt/sources.json`](data/gdelt/sources.json)) containing the specification of data sources and attributes. This operation must be applied before any queries are submitted.

(2): DELETE SOURCES -> Disables attributes from querying; attributes may be enabled again using functionality (1).

(3): CATALOG -> Returns a list of the currently queryable attributes and the operation (categorical, numerical, or spatial) supported for each one.

(4): SEARCH -> Allows specification of a top-k similarity search query. The user must also specify the path to a JSON file containing the query specification (as in `search.json.example` file or [`data/gdelt/search.json`](data/gdelt/search.json)). Once evaluation is complete, results will be available in JSON format (as in [`data/gdelt/search_results.json`](data/gdelt/search_results.json)).

## Launching SimSearch as web service

SimSearch also integrates a REST API and can be deployed as a web service application at a specific port (e.g., 8090) as follows:
```sh
$ java -Dserver.port=8090 -jar target/simsearch-0.0.1-SNAPSHOT.jar --service valid_api_keys.json
```

Option `--service` signifies that a web application will be deployed using [Spring Boot](https://spring.io/projects/spring-boot). All requests should specify a valid API key; a list of all valid keys must be specified by the administrator of the service in a JSON file like this [`example`](valid_api_keys.json.example) and invoked when the service is being launched as above.

Once the SimSearch service is deployed as above, requests can be formulated according to its API documentation (typically accessible at `http://localhost:8090/swagger-ui.html#`). 

Thus, users are able to issue requests to the SimSearch service via a client application (e.g., Python scripts), such as:

- [`MOUNT SOURCES request`](data/gdelt/simsearch-gdelt-sources.py) -> Specifies a JSON with the available data sources and queryable attributes and (if necessary) builts suitable indices on their values. An API key with administrative privileges is required. This operation must be applied before any queries are submitted. Note that multiple data sources of different types (ingested/in-situ) can be specified, as in this [example](data/gdelt/simsearch-multiple-sources.py).

- [`DELETE SOURCES request`](data/gdelt/simsearch-gdelt-delete.py) -> Disables attributes from querying; attributes may be enabled again by using the mounting functionality above. An API key with administrative privileges is required. 

- [`CATALOG request`](data/gdelt/simsearch-gdelt-catalog.py) -> Returns a JSON list of the currently queryable attributes and the operation (categorical, numerical, or spatial) supported for each one. An API key ecognizable by the SimSearch service must be specified.

- [`SEARCH request`](data/gdelt/simsearch-gdelt-query.py) -> Allows specification of a top-k similarity search query using a JSON and an API key recognizable by the SimSearch service. Once evaluation is complete, results will be issued in JSON format.


## License

The contents of this project are licensed under the [Apache License 2.0](https://github.com/smartdatalake/simsearch/blob/master/LICENSE).

