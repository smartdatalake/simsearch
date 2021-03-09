# Multi-dimensional R-tree implementation

This module modifies and extends the R-tree code originally published [here](https://github.com/davidmoten/rtree-multi).

The original code provides an in-memory immutable R-tree implementation for a spatial index in *n* dimensions.

Basic extensions include:

- Support for *multi-dimensional embeddings* of object entities with multiple attributes. Each embedding is a multi-dimensional point of double values.

- Support for *k-nearest neighbor queries* using the [distance browsing paradigm](https://dl.acm.org/doi/10.1145/320248.320255) over the R-tree index. 

- Support for *top-k similarity search* queries using an R-tree index that hold embeddings of objects. This implementation is based on [indexing of multi-metric data](https://doi.org/10.1109/ICDE.2016.7498318).


## License

The contents of the original project are licensed under the [Apache License 2.0](https://github.com/davidmoten/rtree-multi/blob/master/LICENCE).

