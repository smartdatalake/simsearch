## GDELT sample dataset

This [test dataset](sample.csv) contains a small sample of 1000 news articles published in 2019 and extracted from the [GDELT project](https://www.gdeltproject.org/).

Each article represents an entity associated with spatial location (longitude/latitude coordinates in [WGS1984](https://epsg.io/4326)), a numerical attribute (timestamp), as well as three set-valued attributes (Person names, Organization names and Location names). 

This sample dataset exemplifies usage for the [SimSearch implementation](https://github.com/smartdatalake/simsearch) in several settings:

- A [standalone](standalone/) deployment using *rank aggregation* with configurations for mounting [data sources](standalone/sources.json) and a [similarity search query](standalone/search.json) involving *rank aggregation* over attributes *location, timestamp, persons*. [Results](standalone/search_results.json) for this query are available in JSON format.

- A [standalone](standalone/) deployment using *pivot-based* metric indexing of the same [data sources](standalone/sources_pivot.json) and a [pivot-based similarity search query](standalone/search_pivot.json) against the indexed attribute data regarding *location, timestamp, organizations*. Note that word embeddings for the textual, set-valued attribute *organizations* are created on-the-fly using a dictionary of vector representations for terms, e.g., created using [LDA](https://web.stanford.edu/class/linguist289/lda.pdf) or [GloVe](https://nlp.stanford.edu/projects/glove/) in order to be used in the index. [Results](standalone/search_pivot_results.json) for this query are also available in JSON format.

- A [service](service/) deployment using *rank aggregation*. These python scripts show how to instantiate the service by mounting [data sources](service/simsearch-gdelt-sources.py) for various attributes, [append](service/simsearch-gdelt-append.py) an extra attribute to those available at the service, list the [catalog](service/simsearch-gdelt-catalog.py) of queryable attributes, submit a top-*k* similarity [search query](service/simsearch-gdelt-query.py), and also [delete](service/simsearch-gdelt-delete.py) and existing attribute and make it unavailable for queries. There is also a [generic example](service/simsearch-multiple-sources.py) demonstrating how to mount data available from different sources (CSV, DBMS, Elasticsearch), each providing one or multiple queryable attributes over the same entities using common identifiers.
