# SimSearch

## Overview

SimSearch is a Java library providing support for combined similarity search against multi-attribute entities, i.e., datasets with different types of properties (textual/categorical, numerical, spatial, temporal, etc.). The queries enable multi-attribute similarity search for data exploration and may involve different similarity measures per attribute (Jaccard, Euclidean, Manhattan, etc.): 

- *Categorical (set-valued) similarity search*: return data elements with the highest similarity score to the given query set of keywords.
- *Numerical similarity search*: return data elements with the highest similarity score to the given query (numerical) value.
- *Spatial similarity search*: implements k-nearest neighbor search and return data elements closest to the given query (point) location.

Attribute data values may come from diverse data sources, and each one can be either ingested or queried in-situ:

- CSV files can be *ingested* and indices will be constructed in memory (e.g., R-trees for spatial locations, B-trees for numerical values, inverted indices for sets of textual values).
- Data sources queried *in-situ* may involve: 
  + Tables in a [PostgreSQL](https://www.postgresql.org/) database (with [PostGIS extension](https://postgis.net/) if geometries are also stored in a spatial column).
  + Data available from REST APIs, like JSON data hosted in [Elasticsearch](https://www.elastic.co/elasticsearch).

With SimSearch, users can request the top-k results across all examined attributes and get each result ranked by an aggregate similarity score. This library builds specialized indices for each specific attribute type. It supports two alternative methods for similarity search:

- *Top-k rank aggregation*: Under this [paradigm](https://dl.acm.org/doi/10.1145/1391729.1391730), a ranked list of candidate entities is retrieved separately for each attribute based on individual similarity queries at the respective data sources or internal indices. Then, these individual ranked lists are combined to yield the final top-k results and their aggregate scores. 
- *Top-k pivot-based search*: Taking advantage of a [multi-metric indexing mechanism](https://doi.org/10.1109/ICDE.2016.7498318), this method chooses a small number of reference values (a.k.a. pivots) per attribute and can significantly speed up query execution. However, this is feasible only when all involved attributes are ingested and indexed internally using a modified and extended R-tree implementation originally published [here](https://github.com/davidmoten/rtree-multi).

SimSearch can be deployed either as a standalone Java application or as a RESTful web service, as detailed next.

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
$ java -jar target/simsearch-0.3-SNAPSHOT.jar
```

Next, choose a number corresponding to a functionality you want to apply:

(1): MOUNT SOURCES -> Specifies the queryable attributes and (if necessary) builds suitable indices on their values. This mount operation must be applied before any queries are submitted. The user must provide the path to a JSON file containing the specification of data sources and their attributes to be made queryable in SimSearch. Example configuration for enabling SimSearch using rank aggregation is available in `sources.json.example` file or [`data/gdelt/standalone/sources.json`](data/gdelt/standalone/sources.json), which must specify a suitable *metric* distance per attribute. Example configuration for *pivot-based* SimSearch is available in [`data/gdelt/standalone/sources_pivot.json`](data/gdelt/standalone/sources_pivot.json). Note that if set-valued textual attributes (e.g., containing sets of keywords) are involved in SimSearch, they must be transformed into word embeddings using a dictionary of terms that must be also specified in the configuration. For instance, this [`dictionary`](data/gdelt/lda_dictionary.csv) has been constructed with [`Latent Dirichlet Allocation (LDA)`](https://web.stanford.edu/class/linguist289/lda.pdf) over the terms used in the *organizations* attribute of the [`sample dataset`](data/gdelt/sample.csv).    

(2): DELETE SOURCES -> Disables attributes from querying; attributes may be enabled again using functionality (1). This operation is removes the specified attribute(s) when rank aggregation is used. *CAUTION!* For pivot-based SimSearch, this operation drops the centralized index built on all attributes; in this case, the index must be rebuilt from scratch using functionality (1) and involving the desired attributes. 

(3): CATALOG -> Returns a list of the currently queryable attributes and the operation (categorical, numerical, spatial, or pivot-based) supported for each one.

(4): SEARCH -> Allows specification of a top-k similarity search query. The user must also specify the path to a JSON file containing the query specification (as in [`search.json.example`](search.json.example) file or [`data/gdelt/standalone/search.json`](data/gdelt/standalone/search.json) for a search request using rank aggregation). Configuration for search requests using pivot-based SimSearch must specifically define *pivot_based* as the algorithm to be used, as in this [`example`](data/gdelt/standalone/search_pivot.json) of a *pivot-based search request*. In all cases, once evaluation is complete, results will be available in JSON format (as in [`data/gdelt/standalone/search_results.json`](data/gdelt/search_results.json)).

## Launching SimSearch as web service

SimSearch also integrates a REST API and can be deployed as a web service application at a specific port (e.g., 8090) as follows:
```sh
$ java -Dserver.port=8090 -jar target/simsearch-0.3-SNAPSHOT.jar --service
```

Option `--service` signifies that a web application will be deployed using [Spring Boot](https://spring.io/projects/spring-boot). Once the user wishes to make some data source(s) available for similarity search, a new instance of the service is created, which is associated with an auto-generated API key that is returned back to the user. All subsequent requests against this instance of the SimSearch service should specify this API key. Multiple SimSearch instances may be active in parallel but running in isolation, each one responding to requests that specify its own unique API key.

Once an instance of the SimSearch service is deployed as above, requests can be formulated according to the API documentation (typically accessible at `http://localhost:8090/swagger-ui.html#`). 

Thus, users are able to issue requests to an instance of the SimSearch service via a client application (e.g., Python scripts), such as:

- [`MOUNT SOURCES request`](data/gdelt/service/simsearch-gdelt-sources.py) -> Creates a new instance of the SimSearch service against some data source(s). It uses a JSON with the available data sources and their queryable attributes and (if necessary) builds suitable indices on their values. An API key is generated and must be used in any subsequent requests against this instance. This operation must be applied before any queries are submitted. Note that multiple data sources of different types (ingested/in-situ) can be specified, as in this [example](data/gdelt/service/simsearch-multiple-sources.py).

- [`DELETE SOURCES request`](data/gdelt/service/simsearch-gdelt-delete.py) -> Disables attributes from querying; attributes may be enabled again by using the mounting functionality above. An API key referring to this instance of the SimSearch service is required. 

- [`APPEND SOURCES request`](data/gdelt/service/simsearch-gdelt-append.py) -> Specifies a JSON with extra data sources and queryable attributes. This is similar to the aforementioned `mount` request, but instead of creating a new instance, the specified data source(s) (ingested/in-situ) are appended to those already available through this instance of the SimSearch service. The API key initially issued for this instance must be specified in this request. This operation must be applied before any queries are submitted referring to these data sources.

- [`CATALOG request`](data/gdelt/service/simsearch-gdelt-catalog.py) -> Returns a JSON list of the currently queryable attributes and the operation (categorical, numerical, or spatial) supported for each one. An API key referring to this instance of the SimSearch service is required.

- [`SEARCH request`](data/gdelt/service/simsearch-gdelt-query.py) -> Allows specification of a top-k similarity search query using a JSON. An API key referring to this instance of the SimSearch service is required. In case of *in-situ data sources* (e.g., DBMS, Elasticsearch), an optional *filter* may be specified along with the queried attribute. This user-specified condition (written in the dialect of the corresponding data source, e.g., SQL for a DBMS, or filter context for Elasticsearch) may involve any attributes available in that source and is used to filter the underlying data prior to applying similarity search. Once evaluation is complete, results will be issued in JSON format.

In case all data is available in *ElasticSearch*, these [`example scripts`](data/elastic/) demonstrate how to specify a SimSearch instance against various types of ES-indexed atributes and interact with it with top-k similarity search queries. 


## Interactive Data Exploration with the SimSearch REST API and Jupyter notebooks

This [`Jupyter notebook`](data/notebooks/SimSearch_API_demo.ipynb) demonstrates how to interact with a deployed SimSearch service and specify requests.

It also demonstrates how results of multi-attribute SimSearch queries can be visualized in various plots (maps, keyword clouds, histograms) for interactive data exploration.


## Creating and launching a Docker image 

We provide an indicative `Dockerfile` that may be used to create a Docker image (`sdl/simsearch-docker`) from the executable:

```sh
$ docker build -t sdl/simsearch-docker .
```

This docker image can then be used to launch a web service application at a specific port (e.g., 8090) as follows:

```sh
$ docker run -p 8090:8080 sdl/simsearch-docker:latest
```

Once the service is launched, requests can be sent as mentioned above in order to create, manage, and query instances of SimSearch against data source(s).

## Demonstration

We have made available two videos demonstrating the current functionality provided by the SimSearch software:

- A [presentation](https://www.youtube.com/watch?v=18ltkd76B7k) of our paper [Similarity Search over Enriched Geospatial Data](https://dl.acm.org/doi/abs/10.1145/3403896.3403967) accepted in [GeoRich 2020 workshop](https://georich2020.github.io/). This video explains the motivation, outlines the processing flow, and presents results from an experimental validation of this framework.

- Our [demonstration](https://www.youtube.com/watch?v=DDjmYQdxyUc) of the SimSearch functionality as of September 2020, as presented during the mid-term review of the SmartDataLake project.


## Note on R-tree implementation

SimSearch modifies and extends an R-tree implementation originally published [here](https://github.com/davidmoten/rtree-multi).

The original code provides an in-memory immutable R-tree implementation for a spatial index in *n* dimensions.

Basic extensions made for SimSearch include:

- Support for *multi-dimensional embeddings* of object entities with multiple attributes. Each embedding is a multi-dimensional point of double values.

- Support for *k-nearest neighbor queries* using the [distance browsing paradigm](https://dl.acm.org/doi/10.1145/320248.320255) over the R-tree index. 

- Support for *top-k similarity search* queries using an R-tree index that hold embeddings of objects. This implementation is based on [indexing of multi-metric data](https://doi.org/10.1109/ICDE.2016.7498318).


## License

The contents of this project are licensed under the [Apache License 2.0](https://github.com/smartdatalake/simsearch/blob/master/LICENSE).

## Acknowledgement

This software is being developed in the context of the [SmartDataLake](https://smartdatalake.eu/) project. This project has received funding from the European Union’s [Horizon 2020 research and innovation programme](https://ec.europa.eu/programmes/horizon2020/en) under grant agreement No 825041.
