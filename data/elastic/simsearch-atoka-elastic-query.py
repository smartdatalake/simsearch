import requests

# Request to the web service
url = 'http://localhost:8090/simsearch/api/search'

# top-k similarity search query specification
# Possible values for the ranking algorithm: 'threshold' (default); 'no_random_access'; 'partial_random_access'.
# Multiple combinations of weights can be specified; a separate list of top-k results is issued for each such combination
search = {'algorithm':'partial_random_access', 'k':'20','queries':[{'column':'revenue','value':'200000','weights':['0.9','0.7']}, {'column':'location','value':'POINT(11.256 43.774)','weights':['1.0','0.7']}, {'column':'employees','value':'5','weights':['0.6','0.5']}]}

# API key against a running SimSearch instance is required for such requests
# This API key has been issued when the SimSearch instance was created (using an /simsearch/api/index request)
headers = { 'api_key' : 'your_API_KEY_string', 'Content-Type' : 'application/json'}

# Post a request with these parameters
response = requests.post(url, json=search, headers=headers)

# Report a JSON with the qualifying ranked results and their scores
print(response.json())
