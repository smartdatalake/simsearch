import requests

# Request to the web service
url = 'http://localhost:8090/simsearch/api/search'

# top-k similarity search query specification
# Possible values for the ranking algorithm: 'no_random_access'; 'partial_random_access'.
# NOTE: 'threshold' algorithm for rank aggregation CANNOT be used with ES sources, as random access to in-situ queried data is not possible.

# Multiple combinations of weights can be specified; a separate list of top-k results is issued for each such combination
# An optional filter (in Elasticsearch syntax) may be applied on the queried data prior to similarity search separately on a given attribute involved in SimSearch; specify the same filter condition to all queried attributes if you wish to search against a given subset of the dataset.
# This filter is applicable on any existing field, even against attributes not involved in the SimSearch query.

search = {'algorithm':'partial_random_access', 'k':'20','queries':[{'filter': '[{"match":{""legalStatus":"Limited company"}}]','column':'keywords','value':['Computer+science','Electronics','Software','E-commerce'],'weights':['0.4','0.9']},{'filter': '[{"match":{"legalStatus":"Limited company"}}]','column':'revenue','value':'100000','weights':['1.0','0.4']},{'filter': '[{"match":{"legalStatus":"Limited company"}}]','column':'location','value':'POINT (12.4534 41.9029)','weights':['0.9','0.7']}]}

# API key against a running SimSearch instance is required for such requests
# This API key has been issued when the SimSearch instance was created (using an /simsearch/api/index request)
headers = { 'api_key' : 'your_API_KEY_string', 'Content-Type' : 'application/json'}

# Post a request with these parameters
response = requests.post(url, json=search, headers=headers)

# Report a JSON with the qualifying ranked results and their scores
print(response.json())
