# SimSearch

## Overview

SimSearch is a Java library providing functions for combined similarity search against multi-faceted entities, i.e., datasets with different types of attributes (textual/categorical, numerical, spatial, temporal, etc.). The queries enable multi-attribute similarity search for data exploration and may involve different similarity measures per attribute (Jaccard, Euclidean, etc.). This library builts specialized indexes for each specific attribute type. It currently supports the following operationss:

- Categorical similarity search: progressively return data elements with the highest similarity score to the given query set of keywords.
- Numerical similarity search: progressively return data elements with the highest similarity score to the given query (numerical) value.
- Spatial similarity search: implements k-nearest neighbor search and return data elements closest to the given query (point) location.
- Top-k rank aggregation: find top-k results across all examined attributes and issue each result progressively, ranked by an aggregate similarity score.

Datasets can be CSV files and/or tables in a [PostgreSQL](https://www.postgresql.org/) database (with [PostGIS extension](https://postgis.net/) if geometries are also stored in a spatial column).

## Documentation

Javadoc is available [here](https://smartdatalake.github.io/simsearch/).

## Usage

**Step 1**. Download or clone the project:
```sh
$ git clone https://github.com/smartdatalake/simsearch.git
```

**Step 2**. Open terminal inside root folder and compile by running:
```sh
$ mvn clean package
```
**Step 3**. Edit the parameters for the various data sources and their queryable attributes in the `sources.json` file.

**Step 3**. Edit the parameters in the `search.json` file for the various attributes involved in the top-k similarity search query.

**Step 5**. Execute by running:
```sh
$ java -jar target/simsearch-0.0.1-SNAPSHOT-jar-with-dependencies.jar
```

Next, choose a number corresponding to a functionality you want to apply:

(1): MOUNT SOURCES -> Specifies the queryable attributes and (if necessary) builts suitable indices on their values. The user must also specify the path to a JSON file (as in `sources.json.example` file) containing the specification of data sources and attributes. This operation must be applied before any queries are submitted.

(2: DELETE SOURCES -> Disables attributes from querying; attributes may be enabled again using functionality (1).

(3): LIST SOURCES -> Returns a list of the currently queryable attributes and the operation (categorical, numerical, or spatial) supported for each one.

(4): SEARCH -> Allows specification of a top-k similarity search query. The user must also specify the path to a JSON file conataining the query specification (as in `search.json.example` file).

## License

The contents of this project are licensed under the [Apache License 2.0](https://github.com/smartdatalake/simsearch/blob/master/LICENSE).

