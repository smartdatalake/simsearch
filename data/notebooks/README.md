## Interactive Data Exploration with the SimSearch REST API and Jupyter notebooks

This [`Jupyter notebook`](SimSearch_API_demo.ipynb) demonstrates how to interact with a SimSearch service that has been already deployed. 
Make sure that accompanying auxiliary `functions.py` file is located in the same directory with the notebook.

Specifically, this notebook shows how to specify SimSearch requests with Python:

- Connect to the running SimSearch service; 

- Mount data sources to become available for similarity search;

- List the queryable attributes and the supported operations;

- Submit a top-*k* similarity search query involving multiple attributes and several combinations of weights.

Furthermore, results of multi-attribute SimSearch queries may be visualized in various fashions for interactive data exploration using standard Python libraries. There are examples on how to:

- List the top-k results returned for each specified combination of weights;

- Visualize the similarity matrix per result set, measuring the intra-correlation between all pairs of the top-k results;

- Measure inter-correlation between pairs of result sets, each coming from a different combination of weights;

- If a *spatial* attribute is involved in the SimSearch query, render the locations of the resulting entities on map;

- If a textual attribute with *keywords* is involved in the SimSearch query, plot a keyword cloud or a histogram of the most frequent ones;

- If a *numerical* is involved in the SimSearch query, create histograms and boxblots to inspect the distribution of the returned values on this attribute.
