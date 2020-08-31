import requests

# Request to the web service
url = 'http://localhost:8090/simsearch/api/catalog'

# JSON specification may be empty in order to list all available attributes
cat = {}

# API key (no admin priviliges) is required for such requests
# Valid API keys are listed in valid_api_keys.json
headers = { 'api_key' : 'USER_1_API_KEY_string', 'Content-Type' : 'application/json'}

# Post a request with these parameters
response = requests.post(url, json=cat, headers=headers)

# Report a JSON with the queryable attributes and the supported similarity operations
print(response.json())
