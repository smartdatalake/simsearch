import requests

# Request to the web service
url = 'http://localhost:8090/simsearch/api/index'

# JSON specification of the data sources and queryable attributes
index = {'sources':[{'name':'localPath1','type':'csv','directory':'./data/gdelt/'}], 'search':[{'operation':'spatial_knn','source':'localPath1','dataset':'sample.csv','header':'true','separator':',','key_column':'article_id','search_column':['longitude','latitude']}, {'operation':'categorical_topk','source':'localPath1','dataset':'sample.csv','separator':',','token_delimiter':';','header':'true','key_column':'article_id','search_column':'persons'}, {'operation':'numerical_topk','source':'localPath1','dataset':'sample.csv','separator':',','header':'true','key_column':'article_id','search_column':'timestamp'}]}

# API key with admin priviliges is required for such requests
# Valid API keys are listed in valid_api_keys.json
headers = { 'api_key' : 'ADMIN_API_KEY_string', 'Content-Type' : 'application/json'}

# Post a request with these parameters
response = requests.post(url, json=index, headers=headers)

# Provide the resulting message
print(response.text)
