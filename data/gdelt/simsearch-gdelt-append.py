import requests

# Request to the web service
url = 'http://localhost:8090/simsearch/api/append'

# JSON specification of extra data source(s) and their queryable attribute(s)
# In this example, note that the CSV dataset is available at a remote HTTP server
append = {'sources':[{'name':'remotePath1','type':'csv','url':'http://download.smartdatalake.eu/datasets/gdelt/'}], 'search':[{'operation':'categorical_topk','source':'localPath1','dataset':'sample.csv','separator':',','token_delimiter':';','header':'true','key_column':'article_id','search_column':'organizations'}]}

# API key against a running SimSearch instance is required for such requests
# This API key has been issued when the SimSearch instance was created (using an /simsearch/api/index request)
headers = { 'api_key' : 'your_API_KEY_string', 'Content-Type' : 'application/json'}

# Post a request with these parameters
response = requests.post(url, json=append, headers=headers)

# Provide the resulting message with notifications
print(response.json())
