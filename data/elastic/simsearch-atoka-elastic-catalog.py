import requests

# Request to the web service
url = 'http://localhost:8090/simsearch/api/catalog'

# JSON specification may be empty in order to list all available attributes
cat = {}

# API key against a running SimSearch instance is required for such requests
# This API key has been issued when the SimSearch instance was created (using an /simsearch/api/index request)
headers = { 'api_key' : 'your_API_KEY_string', 'Content-Type' : 'application/json'}

# Post a request with these parameters
response = requests.post(url, json=cat, headers=headers)

# Report a JSON with the queryable attributes and the supported similarity operations
print(response.json())
