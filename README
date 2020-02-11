# SimSearch

## Overview

SimSearch is a Java library providing functions for combined similarity search against multi-faceted entities, i.e., datasets with different types of attributes (textual/categorical, numerical, spatial, temporal, etc.). The queries enable multi-criteria similarity search for data exploration and may involve different similarity measures per attribute (Jaccard, Euclidean, etc.). This library builts specialized indexes for each specific attribute type. It currently supports the following functionalities:

- Categorical similarity search: progressively return data elements with the highest similarity score to the given query set of keywords.
- Numerical similarity search: progressively return data elements with the highest similarity score to the given query (numerical) value.
- Top-k rank aggregation: find top-k results across all examined attributes and issue each result progressively, ranked by an aggregate similarity score.

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

**Step 3**. Edit the parameters for the various search queries in the `config.json` file.

**Step 4**. Execute by running:
```sh
$ java -jar target/simsearch-0.0.1-SNAPSHOT-jar-with-dependencies.jar config.json
```

## License

The contents of this project are licensed under the [Apache License 2.0](https://github.com/smartdatalake/simsearch/blob/master/LICENSE).

