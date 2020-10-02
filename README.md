# SimSearch

## Overview

SimSearch is a Java library providing functions for combined similarity search against multi-faceted entities, i.e., datasets with different types of attributes (textual/categorical, numerical, spatial, temporal, etc.). The queries enable multi-attribute similarity search for data exploration and may involve different similarity measures per attribute (Jaccard, Euclidean, etc.). This library builds specialized indexes for each specific attribute type. It currently supports the following operations:

- *Categorical (set-valued) similarity search*: return data elements with the highest similarity score to the given query set of keywords.
- *Numerical similarity search*: return data elements with the highest similarity score to the given query (numerical) value.
- *Spatial similarity search*: implements k-nearest neighbor search and return data elements closest to the given query (point) location.
- *Top-k rank aggregation*: find top-k results across all examined attributes and issue each result ranked by an aggregate similarity score.

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
$ java -jar target/simsearch-0.2-SNAPSHOT.jar
```

Next, choose a number corresponding to a functionality you want to apply:

(1): MOUNT SOURCES -> Specifies the queryable attributes and (if necessary) builds suitable indices on their values. The user must also specify the path to a JSON file (as in `sources.json.example` file or [`data/gdelt/sources.json`](data/gdelt/sources.json)) containing the specification of data sources and attributes. This operation must be applied before any queries are submitted.

(2): DELETE SOURCES -> Disables attributes from querying; attributes may be enabled again using functionality (1).

(3): CATALOG -> Returns a list of the currently queryable attributes and the operation (categorical, numerical, or spatial) supported for each one.

(4): SEARCH -> Allows specification of a top-k similarity search query. The user must also specify the path to a JSON file containing the query specification (as in `search.json.example` file or [`data/gdelt/search.json`](data/gdelt/search.json)). Once evaluation is complete, results will be available in JSON format (as in [`data/gdelt/search_results.json`](data/gdelt/search_results.json)).

## Launching SimSearch as web service

SimSearch also integrates a REST API and can be deployed as a web service application at a specific port (e.g., 8090) as follows:
```sh
$ java -Dserver.port=8090 -jar target/simsearch-0.2-SNAPSHOT.jar --service
```

Option `--service` signifies that a web application will be deployed using [Spring Boot](https://spring.io/projects/spring-boot). Once the user wishes to make some data source(s) available for similarity search, a new instance of the service is created, which is associated with an auto-generated API key that is returned back to the user. All subsequent requests against this instance of the SimSearch service should specify this API key. Multiple instances may be active in parallel, each one responding to requests that specify its own unique API key.

Once an instance of the SimSearch service is deployed as above, requests can be formulated according to the API documentation (typically accessible at `http://localhost:8090/swagger-ui.html#`). 

Thus, users are able to issue requests to an instance of the SimSearch service via a client application (e.g., Python scripts), such as:

- [`MOUNT SOURCES request`](data/gdelt/simsearch-gdelt-sources.py) -> Creates a new instance of the SimSearch service against some data source(s). It uses a JSON with the available data sources and their queryable attributes and (if necessary) builds suitable indices on their values. An API key is generated and must be used in any subsequent requests against this instance. This operation must be applied before any queries are submitted. Note that multiple data sources of different types (ingested/in-situ) can be specified, as in this [example](data/gdelt/simsearch-multiple-sources.py).

- [`DELETE SOURCES request`](data/gdelt/simsearch-gdelt-delete.py) -> Disables attributes from querying; attributes may be enabled again by using the mounting functionality above. An API key referring to this instance of the SimSearch service is required. 

- [`APPEND SOURCES request`](data/gdelt/simsearch-gdelt-append.py) -> Specifies a JSON with extra data sources and queryable attributes. This is similar to the aforementioned `mount` request, but instead of creating a new instance, the specified data source(s) (ingested/in-situ) are appended to those already available through this instance of the SimSearch service. The API key initially issued for this instance must be specified in this request. This operation must be applied before any queries are submitted referring to these data sources.

- [`CATALOG request`](data/gdelt/simsearch-gdelt-catalog.py) -> Returns a JSON list of the currently queryable attributes and the operation (categorical, numerical, or spatial) supported for each one. An API key referring to this instance of the SimSearch service is required.

- [`SEARCH request`](data/gdelt/simsearch-gdelt-query.py) -> Allows specification of a top-k similarity search query using a JSON. An API key referring to this instance of the SimSearch service is required. Once evaluation is complete, results will be issued in JSON format.

## Creating and launching a Docker image 

We provide an indicative `Dockerfile` that may be used to create a Docker image (`sdl/simsearch-docker`) from the executable:

```sh
$ docker build -t sdl/simsearch-docker .
```

This docker image can then be used to launch a web service application at a specific port (e.g., 8090) as follows:

```sh
$ docker run -p 8090:8080 sdl/simsearch-docker:latest --service
```

Once the service is launched, requests can be sent as mentioned above in order to create, manage, and query instances of SimSearch against data source(s).

## Demonstration

We have made available two videos demonstrating the current functionality provided by the SimSearch software:

- A [presentation](https://www.youtube.com/watch?v=18ltkd76B7k) of our paper [Similarity Search over Enriched Geospatial Data](https://dl.acm.org/doi/abs/10.1145/3403896.3403967) accepted in [GeoRich 2020 workshop](https://georich2020.github.io/). This video explains the motivation, outlines the processing flow, and presents results from an experimental validation of this framework.

- Our [demonstration](https://www.youtube.com/watch?v=DDjmYQdxyUc) of the SimSearch functionality as of September 2020, as presented during the mid-term review of the SmartDataLake project.

## License

The contents of this project are licensed under the [Apache License 2.0](https://github.com/smartdatalake/simsearch/blob/master/LICENSE).

## Acknowledgement

This software is being developed in the context of the [SmartDataLake](https://smartdatalake.eu/) project. This project has received funding from the European Union’s [Horizon 2020 research and innovation programme](https://ec.europa.eu/programmes/horizon2020/en) under grant agreement No 825041.
