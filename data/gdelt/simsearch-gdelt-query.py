import requests

# Request to the web service
url = 'http://localhost:8090/simsearch/api/search'

# top-k similarity search query specification
# Possible values for the ranking algorithm: 'threshold' (default); 'no_random_access'; 'partial_random_access'.
# Multiple combinations of weights can be specified; a separate list of top-k results is issued for each such combination
search = {'k':'5', 'algorithm':'partial_random_access', 'queries':[{'column':'persons','value': ['joe biden','donald trump'],'weights':['1.0','0.8']}, {'column':'timestamp','value':'20191104084500','weights':['1.0','0.4']}, {'column':['longitude','latitude'],'value':'POINT(-74.94 42.15)','weights':['1.0','0.7']}]}

# API key against a running SimSearch instance is required for such requests
# This API key has been issued when the SimSearch instance was created (using an /simsearch/api/index request)
headers = { 'api_key' : 'your_API_KEY_string', 'Content-Type' : 'application/json'}

# Post a request with these parameters
response = requests.post(url, json=search, headers=headers)

# Report a JSON with the qualifying ranked results and their scores
print(response.json())
