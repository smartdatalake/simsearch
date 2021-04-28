import requests

# Request to the web service to create a new instance of SimSearch
url = 'http://localhost:8090/simsearch/api/index'

# JSON specification of the data sources and queryable attributes
index = {'sources':[{'name':'localPath1','type':'csv','directory':'./data/gdelt/'}], 'search':[{'operation':'spatial_knn','source':'localPath1','dataset':'sample.csv','header':'true','separator':',','key_column':'article_id','search_column':['longitude','latitude'], 'alias_column':'position'}, {'operation':'categorical_topk','source':'localPath1','dataset':'sample.csv','separator':',','token_delimiter':';','header':'true','key_column':'article_id','search_column':'persons'}, {'operation':'numerical_topk','source':'localPath1','dataset':'sample.csv','separator':',','header':'true','key_column':'article_id','search_column':'negative_sentiment'},{'operation':'numerical_topk','source':'localPath1','dataset':'sample.csv','separator':',','header':'true','key_column':'article_id','search_column':'positive_sentiment'},{'operation':'temporal_topk','source':'localPath1','dataset':'sample.csv','separator':',','header':'true','key_column':'article_id','search_column':'timestamp'}]}

# No API key is required when submitting this request to create a new instance
# A new API key will be generated once this request completes successfully
headers = {'Content-Type' : 'application/json'}

# Post a request with these parameters
response = requests.post(url, json=index, headers=headers)

# Provide the resulting message
# IMPORTANT! Save the API key returned with this message and use it in all subsequent requests against this instance of SimSearch
print(response.json())