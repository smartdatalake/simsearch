# SimSearch

## Overview

SimSearch is an open-source software for top-*k* similarity search over multi-attribute entity profiles possibly residing in different, remote, and heterogeneous data sources.

SimSearch is developed in Java and provides support for combined similarity search against multi-attribute entities, i.e., datasets with different types of properties (textual/categorical, numerical, spatial, temporal, etc.). The queries enable multi-attribute similarity search for data exploration and may involve different similarity measures per attribute (Jaccard, Euclidean, Manhattan, etc.): 

- *Categorical (set-valued) similarity search*: return data elements with the highest similarity score to the given query set of keywords.
- *Textual (string) similarity search*: return data elements with the highest similarity score to the given query string.
- *Numerical similarity search*: return data elements with the highest similarity score to the given query (numerical) value.
- *Spatial similarity search*: implements k-nearest neighbor search and returns data elements closest to the given query (point) location.
- *Temporal similarity search*: return data elements with the highest similarity score to the given query (date/time) value.

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
$ java -jar target/simsearch-0.5-SNAPSHOT.jar
```

Next, choose a number corresponding to a functionality you want to apply:

(1): MOUNT SOURCES -> Specifies the queryable attributes and (if necessary) builds suitable indices on their values. This mount operation must be applied before any queries are submitted. The user must provide the path to a JSON file containing the specification of data sources and their attributes to be made queryable in SimSearch. Example configuration for enabling SimSearch using rank aggregation is available in `sources.json.example` file or [`data/gdelt/standalone/sources.json`](data/gdelt/standalone/sources.json), which must specify a suitable *metric* distance per attribute. Example configuration for *pivot-based* SimSearch is available in [`data/gdelt/standalone/sources_pivot.json`](data/gdelt/standalone/sources_pivot.json). Note that if set-valued textual attributes (e.g., containing sets of keywords) are involved in SimSearch, they must be transformed into word embeddings using a dictionary of terms that must be also specified in the configuration. For instance, this [`dictionary`](data/gdelt/lda_dictionary.csv) has been constructed with [`Latent Dirichlet Allocation (LDA)`](https://web.stanford.edu/class/linguist289/lda.pdf) over the terms used in the *organizations* attribute of the [`sample dataset`](data/gdelt/sample.csv).    

(2): DELETE SOURCES -> Disables attributes from querying; attributes may be enabled again using functionality (1). This operation is removes the specified attribute(s) when rank aggregation is used. *CAUTION!* For pivot-based SimSearch, this operation drops the centralized index built on all attributes; in this case, the index must be rebuilt from scratch using functionality (1) and involving the desired attributes. 

(3): CATALOG -> Returns a list of the currently queryable attributes and the operation (categorical, numerical, spatial, or pivot-based) supported for each one.

(4): SEARCH -> Allows specification of a top-k similarity search query. The user must also specify the path to a JSON file containing the query specification (as in [`search.json.example`](search.json.example) file or [`data/gdelt/standalone/search.json`](data/gdelt/standalone/search.json) for a search request using rank aggregation). Configuration for search requests using pivot-based SimSearch must specifically define *pivot_based* as the algorithm to be used, as in this [`example`](data/gdelt/standalone/search_pivot.json) of a *pivot-based search request*. In all cases, once evaluation is complete, results will be available in JSON format (as in [`data/gdelt/standalone/search_results.json`](data/gdelt/standalone/search_results.json)).

(5): SQL TERMINAL -> This terminal-based front-end enables users to type *conjunctive SQL-like queries*, issue them against the locally running SimSearch instance, and readily inspect the query results. Please consult details on this [SQL syntax](#sql-syntax) specifically customized for top-*k* similarity search. 


## Launching SimSearch as web service

SimSearch also integrates a REST API and can be deployed as a web service application at a specific port (e.g., 8090) as follows:

```sh
$ java -Dserver.port=8090 -jar target/simsearch-0.5-SNAPSHOT.jar --service
```

Option `--service` signifies that a web application will be deployed using [Spring Boot](https://spring.io/projects/spring-boot). Once the user wishes to make some data source(s) available for similarity search, a new instance of the service is created, which is associated with an auto-generated API key that is returned back to the user. All subsequent requests against this instance of the SimSearch service should specify this API key. Multiple SimSearch instances may be active in parallel but running in isolation, each one responding to requests that specify its own unique API key.

Once an instance of the SimSearch service is deployed as above, requests can be formulated according to the API documentation (typically accessible at `http://localhost:8090/swagger-ui.html#`). 

Thus, users are able to issue requests to an instance of the SimSearch service via a client application (e.g., Python scripts), such as:

- [`MOUNT SOURCES request`](data/gdelt/service/simsearch-gdelt-sources.py) -> Creates a new instance of the SimSearch service against some data source(s). It uses a JSON with the available data sources and their queryable attributes and (if necessary) builds suitable indices on their values. An API key is generated and must be used in any subsequent requests against this instance. This operation must be applied before any queries are submitted. Note that multiple data sources of different types (ingested/in-situ) can be specified, as in this [example](data/gdelt/service/simsearch-multiple-sources.py).

- [`DELETE SOURCES request`](data/gdelt/service/simsearch-gdelt-delete.py) -> Disables attributes from querying; attributes may be enabled again by using the mounting functionality above. An API key referring to this instance of the SimSearch service is required. 

- [`APPEND SOURCES request`](data/gdelt/service/simsearch-gdelt-append.py) -> Specifies a JSON with extra data sources and queryable attributes. This is similar to the aforementioned `mount` request, but instead of creating a new instance, the specified data source(s) (ingested/in-situ) are appended to those already available through this instance of the SimSearch service. The API key initially issued for this instance must be specified in this request. This operation must be applied before any queries are submitted referring to these data sources.

- [`CATALOG request`](data/gdelt/service/simsearch-gdelt-catalog.py) -> Returns a JSON list of the currently queryable attributes and the operation (categorical, numerical, or spatial) supported for each one. An API key referring to this instance of the SimSearch service is required.

- [`SEARCH request`](data/gdelt/service/simsearch-gdelt-query.py) -> Allows specification of a top-*k* similarity search query using a JSON. An API key referring to this instance of the SimSearch service is required. In case of *in-situ data sources* (e.g., DBMS, Elasticsearch), an optional *filter* may be specified along with the queried attribute. This user-specified condition (written in the dialect of the corresponding data source, e.g., SQL for a DBMS, or filter context for Elasticsearch) may involve any attributes available in that source and is used to filter the underlying data prior to applying similarity search. Once evaluation is complete, results will be issued in JSON format.

In case all data is available in *ElasticSearch*, these [`example scripts`](data/elastic/) demonstrate how to specify a SimSearch instance against various types of ES-indexed atributes and interact with it with top-k similarity search queries. 


## Value specification in search requests

SimSearch supports several options in specifying query values in *search requests*. The following _examples_ indicate these alternative specifications for the various types of attributes involved in SimSearch queries:

- *GEOLOCATION*. Currently, this applies only to 2-dimensional point locations with long/lat coordinates:
	1) _Well-Known Text representation_: "POINT (11.256 43.774)"
	2) _String of comma-separated coordinates_: "11.256, 43.774"
	3) _Array of numerical coordinates_: [11.256, 43.774]
	4) _Array of string values representing coordinates_: ["11.256", "43.774"]

- *NUMBER*. This data type concerns numerical values specified as:
	1) An _integer_ value: 5
	2) A _double_ value: 124.68
	3) A _string containing a numerical value_: "124.68"

- *DATE_TIME*. This data type concerns temporal values specified as:
	1) A _date_ value in several common formats, e.g.: "2015-03-24", "24/03/2015", "2015-03"
	2) A _date time_ value, e.g.: "2015-03-24 14:03:42", "2015-03-24 08:25:19+03", "2015-03-24T08:25:19+03Z"
	3) A _time_ value, e.g.: "14:03:42"
	4) A _timestamp_ value (optionally with milliseconds): "2015-03-24 14:03:42.366"
	
- *KEYWORD_SET*. Such values typically represent sets of keywords used in categorical (set-valued) search and can be specified as:
	1) _Array of strings_: ["Computer+science", "Electronics", "Software", "E-commerce"]
	2) String of _comma-separated keywords_: "Computer+science, Electronics, Software, E-commerce"

- *STRING*. For textual (string) similarity search, simply specify:
	1) A _string_ enclosed in quotes: "Big Ben"


## SQL syntax

When running SimSearch as a *standalone* application, its terminal-based interface allows users to submit *conjunctive SQL-like queries* and interactively browse the results. In particular, users can write `SELECT` statements of the following syntax (optional clauses are enclosed in brackets):

```sh
SELECT *, [ extra_attrX [, ...] ]
    [ FROM running_instance ]
      WHERE attr_name1 ~= 'attr_value1' [ AND ...]
    [ WEIGHTS weight_value1 [, ...] ]
    [ ALGORITHM { threshold | partial_random_access | no_random_access | pivot_based } ]
    [ LIMIT count ] ;
```

More specifically:

- The `SELECT` clause returns all attributes involved in the *similarity criteria* of this query, as well as the entity identifiers, the entity names (if available), as well as the rank and the overall score assigned by the similarity search algorithm. *Extra attribute names* (but neither functions nor other expressions) may be specified, even if not involved in similarity conditions; however, such extra attributes must have been [mounted as data sources](#standalone-execution) for the running SimSearch instance. 

- The `FROM` clause currently targets the _locally running SimSearch instance_ by default, so it may be omitted without error. The SimSearch instance must have been created by [mounting data sources in standalone mode](#standalone-execution).

- The `WHERE` clause specifies the conditions. _At least one similarity condition_ must be specified with the `~=` operator involving an attribute name and the respective query value always enclosed within single quotes regardless of its data type (e.g., '12.4', 'POINT (-74.94 42.15)', '2021-12-20'). Multiple such similarity operations can be specified against different attributes conjuncted with the `AND` logical operator. Furthermore, in case that data sources can be queried in situ, such as a DBMS that _natively supports SQL_ queries (e.g., PostgreSQL), _extra boolean filters_ can be specified following standard SQL syntax (involving comparison operators like `>` or `=`, matching against a list of values with the `IN` operator, etc.). Subqueries are _not_ supported in conditions.

- Optionally, `WEIGHTS` may be specified as a list of comma separated real numbers between 0 and 1. Each such weight corresponds to an attribute in the `WHERE` clause involved in similarity search operations (i.e., operator `~=`). The order of these weight values corresponds to the exact order in which each attribute is specified in the `WHERE` clause. If omitted, a weight is automatically assigned to each attribute involved in similarity conditions according to the distribution of its values.

- Optionally, users may choose the similarity method with the `ALGORITHM` clause. If omitted, the `threshold` algorithm will be used by default, if random access to raw data is supported by the specified sources.

- Finally, the `LIMIT` clause may be used to specify the number *k* of results with the highest scores to be returned. If this clause is omitted, the top-50 results are returned by default.

These [`example SQL statements`](data/gdelt/standalone/queries.sql) demonstrate how to specify such queries through the terminal over a locally running instance of SimSearch. In the listing of returned results, the applied weights are shown in brackets next to the names of attributes involved in the similarity criteria.

Finally, users may specify the internal parameter setting `query_timeout` regarding the maximum response time allowed per query (default value: 10000 milliseconds). If this deadline is reached during evaluation of a query, the best (i.e., approximately scored) results found so far will be fetched. This timeout value is specified *in milliseconds* as in this example:

```sh
SET query_timeout 20000;
```

Then, execution of any newly submitted queries will timeout after 20 seconds at the latest, issuing all currently collected results, but with possibly approximate scores and ranks.


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

- Our [demonstration](https://www.youtube.com/watch?v=aj_RNiIPF8w) of the SimSearch UI as presented at the [SIMPLIFY Workshop](https://simplify2021.imsi.athenarc.gr/) in March 2021. This demo exemplifies [Multi-Attribute Similarity Search for Interactive Data Exploration](https://simplify2021.imsi.athenarc.gr/papers/SIMPLIFY_2021_paper_11.pdf) through a web interface. It shows how users can specify parameters and their preferences for similarity search, and also how they can visually inspect and compare the results through appropriate visualizations for the different types of attributes involved.


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
