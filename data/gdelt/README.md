## GDELT sample dataset

This test dataset contains a small sample of 1000 news articles published in 2019 and extracted from the [GDELT project](https://www.gdeltproject.org/).

Each article represents an entity associated with spatial location (longitude/latitude coordinates in [WGS1984](https://epsg.io/4326)), a numerical attribute (timestamp), as well as three set-valued attributes (Person names, Organization names and Location names). 

This sample dataset can be used for testing the [SimSearch implementation](https://github.com/smartdatalake/simsearch) with configurations similar for [data sources](sources.json) and [similarity search queries](search.json) to those available in this folder.
