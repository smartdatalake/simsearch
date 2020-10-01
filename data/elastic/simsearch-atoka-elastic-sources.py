import requests

# Request to the web service to create a new instance of SimSearch
url = 'http://localhost:8090/simsearch/api/index'

# Example JSON specification of the multiple data sources and queryable attributes
# Three different attributes are specified available from ElasticSearch REST API service (URL) and accessed with username/password credentials.
index = {'sources':[{'name':'myElasticSearchAPI', 'type':'restapi', 'url':'http://localhost:9200/companies/_search?pretty', 'username':'myElasticUserName', 'password':'myElasticPassword'}], 'search':[{'operation':'numerical_topk', 'source':'myElasticSearchAPI', 'dataset':'companies', 'key_column':'id', 'search_column':'revenue'}, {'operation':'spatial_knn', 'source':'myElasticSearchAPI', 'dataset':'companies', 'key_column':'id', 'search_column':'location'}, {'operation':'categorical_topk', 'source':'myElasticSearchAPI', 'dataset':'companies', 'key_column':'id', 'search_column':'keywords'}]}

# No API key is required when submitting this request to create a new instance
# A new API key will be generated once this request completes successfully
headers = {'Content-Type' : 'application/json'}

# Post a request with these parameters
response = requests.post(url, json=index, headers=headers)

# Provide the resulting message
# IMPORTANT! Save the API key return with this message and use it in all subsequent requests against this instance of SimSearch
print(response.json())