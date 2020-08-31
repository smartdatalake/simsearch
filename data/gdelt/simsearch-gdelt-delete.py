import requests

# Request to the web service
url = 'http://localhost:8090/simsearch/api/delete'

# JSON specification of the attribute(s) to be removed
delete = {'remove':[{'operation':'numerical_topk', 'column':'timestamp'}]}

# API key with admin priviliges is required for such requests
# Valid API keys are listed in valid_api_keys.json
headers = { 'api_key' : 'ADMIN_API_KEY_string', 'Content-Type' : 'application/json'}

# Post a request with these parameters
response = requests.post(url, json=delete, headers=headers)

# Provide the resulting message
print(response.text)
