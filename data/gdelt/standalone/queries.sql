--------------------------------------------------------------------------------------
-- TOP-K SIMILARITY SEARCH EXAMPLE QUERIES SPECIFIED AS SQL-LIKE SELECT STATEMENTS 
-- Before running any query, a SimSearch instance should be mounted with GDELT data on various attributes 
-- (according to the settings specified in files sources.json and sources_pivot.json).
--------------------------------------------------------------------------------------

-- Q1: Top-5 articles mentioning specific persons and published around a given date. No weights specified per attribute, so ranking will be based on automatically assigned weights:

SELECT * FROM running_instance
WHERE persons ~= 'joe biden, donald trump'  AND  timestamp ~= '2019-11-04'
LIMIT 5;


-- Q2: Top-10 articles published close to a specific location and have a positive sentiment around 1.5. Ranking will be based on the specified weights per attribute using the given processing algorithm. Two extra columns (persons, timestamp) will be reported in the results:

SELECT *, persons, timestamp
FROM running_instance
WHERE position ~= 'POINT (-74.94 42.15)'
AND positive_sentiment ~= '1.5'
WEIGHTS 0.8, 0.95
ALGORITHM pivot_based
LIMIT 10;


-- Q2a: The same query specifying another ranking algorithm and different weights:

SELECT *, persons, timestamp
FROM running_instance
WHERE position ~= 'POINT (-74.94 42.15)'
AND positive_sentiment ~= '1.5'
WEIGHTS 0.9, 0.3
ALGORITHM partial_random_access
LIMIT 10;


-- Q3: Search for articles most similar on three attributes to the user-specified values. No specifications regarding the SimSearch instance (the running one will be targeted by default), weights (to be automatically assigned), algorithm (threshold-based method will be used) or limit on the top-k results (will be set to 50):

SELECT *, negative_sentiment
WHERE position ~= 'POINT (-77.04 38.9))'
AND timestamp ~= '2019-11-04 08:45:00'
AND persons ~= 'joe biden, donald trump' ;


-- Q4: Top-10 articles most similar to the user-specified values on four attributes of different types, employing a particular algorithm. Note that the timestamp attribute is considered as numerical by this algorithm, so the query value should not be given in temporal formatting, but as a number:

SELECT *
FROM running_instance
WHERE position ~= 'POINT(-74.94 42.15)'
AND organizations ~= 'washington post, cnn'
AND negative_sentiment ~= '4.5'
AND timestamp ~= '20191113080000'
WEIGHTS 0.6, 0.8, 0.7, 0.9
ALGORITHM pivot_based
LIMIT 10;


-- Q5: Query fails because it involves an attribute not accessible by the specified algorithm: 

SELECT *
FROM running_instance
WHERE position ~= 'POINT (-77.04 38.9))'
AND organizations ~= 'washington post, cnn'
WEIGHTS 0.5, 0.8
ALGORITHM no_random_access
LIMIT 10;


-- Q5a: But the last query works when choosing a suitable algorithm that can access all specified attributes:

SELECT *
FROM running_instance
WHERE position ~= 'POINT (-77.04 38.9))'
AND organizations ~= 'washington post, cnn'
WEIGHTS 0.5, 0.8
ALGORITHM pivot_based
LIMIT 10;


-- Q5b: For ingested data sources (as in this example setting that uses CSV files as input), any extra boolean filters (e.g., on positive_sentiment and negative_sentiment) specified in the last query will be ignored during processing:

SELECT *
FROM running_instance
WHERE position ~= 'POINT (-77.04 38.9))'
AND organizations ~= 'washington post, cnn'
AND negative_sentiment BETWEEN 1 AND 3.5
AND positive_sentiment > 2.75
WEIGHTS 0.5, 0.8
ALGORITHM pivot_based
LIMIT 10;


-- Q6: Query fails because it involves no similarity condition at all:

SELECT *
FROM running_instance
WHERE negative_sentiment BETWEEN 1 AND 3.5
AND positive_sentiment > 2.75
LIMIT 10;
