import requests

# Request to the web service
url = 'http://localhost:8090/simsearch/api/append'

# JSON specification of extra data source(s) and their queryable attribute(s)
# In this example, the source is already defined by the previously issued /simsearch/api/index request that created this instance of SimSearch
append = {'search':[{'operation':'numerical_topk', 'source':'myElasticSearchAPI', 'dataset':'companies', 'key_column':'id', 'search_column':'employees'}]}

# API key against a running SimSearch instance is required for such requests
# This API key has been issued when the SimSearch instance was created (using an /simsearch/api/index request)
headers = {'api_key' : 'your_API_KEY_string', 'Content-Type' : 'application/json'}

# Post a request with these parameters
response = requests.post(url, json=append, headers=headers)

# Provide the resulting message with notifications
print(response.json())
