--------------------------------------------------------------------------------------------
-- TOP-K SIMILARITY SEARCH EXAMPLE QUERIES SPECIFIED AS SQL-LIKE SELECT STATEMENTS 
-- Before running any query, a SimSearch instance should be mounted on various attributes 
-- all residing in ElasticSearch indices (according to the settings in file sources.json).
--------------------------------------------------------------------------------------------

-- Q1: Top-5 companies located close to specific coordinates and founded around a given date. No weights specified per attribute, so ranking will be based on automatically assigned weights:

SELECT *
FROM local 
WHERE location ~= 'POINT (16.87 41.11)'
AND active_since ~= '2015-01-01'
ALGORITHM no_random_access
LIMIT 5;

-- Q2: Top-10 companies characterized with the given keywords, having a given annual revenue close the given value and with around 1 employee. A different weight is specified per similarity attribute and a specific ranking method will be used. Results will also report an extra attribute (active_since) not used in the similarity criteria:

SELECT * , active_since
FROM local 
WHERE revenue ~= '200000'
AND keywords ~= 'Trade, Real+estate'
AND employees ~= '1'
WEIGHTS 1.0, 0.9, 1.0
ALGORITHM no_random_access
LIMIT 10;


-- Q3: Find top-10 companies close to a particular location and employing around 10 persons. Reported results should include two more columns (revenue, active_since) not involved in the similarity criteria. However, extra boolean criteria (not involving similarity, but comparison operators in SQL like =, >=, or BETWEEN) will be ignored during evaluation against ElasticSearch data sources:

SELECT * , revenue, active_since
FROM local 
WHERE location ~= 'POINT (13.550303 37.299998)'
AND employees ~= 10
AND revenue BETWEEN 100000 AND 500000
AND active_since >= '1980-05-01'
AND atecoLeafCode = '41.20.00'
WEIGHTS 0.8, 0.9
ALGORITHM partial_random_access
LIMIT 10;


-- Q3a: If these extra boolean filters are specified in JSON format as applicable to ElasticSearch, then this query returns results:

SELECT * , revenue, active_since
FROM local 
WHERE location ~= 'POINT (13.550303 37.299998)'
AND employees ~= 10
AND JsonFilter('[{"range":{"active_since":{"gte":"1980-05-01"}}}, 
{"range": {"revenue": {"gte": 100000,"lte": 500000,"boost": 2.0}}},
{"match":{"atecoLeafCode":"41.20.00"}}]')
WEIGHTS 0.8, 0.9
ALGORITHM partial_random_access
LIMIT 10;
