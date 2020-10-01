import requests

# Request to the web service
url = 'http://localhost:8090/simsearch/api/delete'

# JSON specification of the attribute(s) to be removed from those available in the service
delete = {'remove':[{'operation':'numerical_topk', 'column':'employees'}]}

# API key against a running SimSearch instance is required for such requests
# This API key has been issued when the SimSearch instance was created (using an /simsearch/api/index request)
headers = { 'api_key' : 'your_API_KEY_string', 'Content-Type' : 'application/json'}

# Post a request with these parameters
response = requests.post(url, json=delete, headers=headers)

# Provide the resulting message with notifications
print(response.json())
